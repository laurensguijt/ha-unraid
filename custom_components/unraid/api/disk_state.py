"""Disk state management for Unraid."""
from __future__ import annotations

import logging
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any
from enum import Enum

_LOGGER = logging.getLogger(__name__)

class DiskState(Enum):
    """Disk power state."""
    ACTIVE = "active"
    STANDBY = "standby"
    UNKNOWN = "unknown"

class DiskStateManager:
    """Manager for disk state tracking with enhanced detection."""
    
    def __init__(self, instance: Any):
        self._instance = instance
        self._states: Dict[str, DiskState] = {}
        self._last_check: Dict[str, datetime] = {}
        self._spindown_delays: Dict[str, int] = {}
        self._lock = asyncio.Lock()
        self._device_types: Dict[str, str] = {}  # Track device types (nvme, sata, etc)

    async def get_disk_state(self, device: str) -> DiskState:
        """Get disk state using multiple methods with SMART as primary."""
        try:
            # Skip system paths and invalid disk names
            if device in ["disks", "remotes", "addons", "rootshare"]:
                return DiskState.UNKNOWN

            # Format device path correctly
            device_path = device
            if not device.startswith('/dev/'):
                if device.startswith('sd'):
                    device_path = f"/dev/{device}"
                elif device == "cache":
                    device_path = "/dev/nvme0n1"
                    self._device_types[device] = 'nvme'
                    return DiskState.ACTIVE
                elif device.startswith("disk"):
                    try:
                        disk_num = int(''.join(filter(str.isdigit, device)))
                        device_path = f"/dev/sd{chr(ord('b') + disk_num - 1)}"
                    except ValueError:
                        _LOGGER.error("Invalid disk number in %s", device)
                        return DiskState.UNKNOWN
                else:
                    _LOGGER.error("Unrecognized device format: %s", device)
                    return DiskState.UNKNOWN

            if device_path not in self._device_types:
                if any(x in str(device_path).lower() for x in ['nvme', 'nvm']):
                    self._device_types[device_path] = 'nvme'
                    _LOGGER.debug("NVMe device detected: %s", device_path)
                    return DiskState.ACTIVE
                else:
                    self._device_types[device_path] = 'sata'
                    _LOGGER.debug("SATA device detected: %s", device_path)

            device_type = self._device_types[device_path]
            state = DiskState.UNKNOWN

            if device_type == 'sata':
                # Try SMART first
                smart_cmd = f"smartctl -n standby -j {device_path}"
                _LOGGER.debug("Executing SMART command for %s: %s", device_path, smart_cmd)
                
                result = await self._instance.execute_command(smart_cmd)
                _LOGGER.debug(
                    "SMART command result for %s: exit_code=%d, stdout='%s', stderr='%s'",
                    device_path,
                    result.exit_status,
                    result.stdout.strip() if result.stdout else "None",
                    result.stderr.strip() if result.stderr else "None"
                )

                # Interpret SMART results
                if result.exit_status == 2:
                    _LOGGER.debug("SMART reports device %s in STANDBY (exit code 2)", device_path)
                    state = DiskState.STANDBY
                elif result.exit_status == 0:
                    _LOGGER.debug("SMART reports device %s is ACTIVE (exit code 0)", device_path)
                    state = DiskState.ACTIVE
                else:
                    # Fallback to hdparm if SMART check fails
                    _LOGGER.debug("SMART check failed (exit code %d), trying hdparm for %s", 
                                result.exit_status, device_path)
                    try:
                        hdparm_cmd = f"hdparm -C {device_path}"
                        _LOGGER.debug("Executing hdparm command: %s", hdparm_cmd)
                        
                        result = await self._instance.execute_command(hdparm_cmd)
                        _LOGGER.debug(
                            "hdparm result for %s: exit_code=%d, stdout='%s', stderr='%s'",
                            device_path,
                            result.exit_status,
                            result.stdout.strip() if result.stdout else "None",
                            result.stderr.strip() if result.stderr else "None"
                        )

                        output = result.stdout.lower()
                        if "active" in output or "idle" in output:
                            _LOGGER.debug("hdparm reports device %s is ACTIVE", device_path)
                            state = DiskState.ACTIVE
                        elif "standby" in output:
                            _LOGGER.debug("hdparm reports device %s is in STANDBY", device_path)
                            state = DiskState.STANDBY
                        else:
                            _LOGGER.warning(
                                "Both SMART and hdparm failed to determine state for %s (smart_exit=%d, hdparm_output='%s'), assuming ACTIVE",
                                device_path,
                                result.exit_status,
                                output.strip()
                            )
                            state = DiskState.ACTIVE
                    except Exception as err:
                        _LOGGER.warning(
                            "hdparm check failed for %s: %s, SMART exit was %d, assuming ACTIVE",
                            device_path,
                            err,
                            result.exit_status
                        )
                        state = DiskState.ACTIVE
            else:
                # NVMe drives are always active
                state = DiskState.ACTIVE
                _LOGGER.debug("NVMe device %s: always active", device_path)

            # Cache the state
            self._states[device_path] = state
            self._last_check[device_path] = datetime.now(timezone.utc)
            
            # Log final decision with full context
            _LOGGER.debug(
                "Final state for %s (%s): %s (SMART result: %s, hdparm result: %s)",
                device_path,
                device_type,
                state.value,
                getattr(result, 'exit_status', 'not_run'),
                getattr(result, 'stdout', 'not_run').strip() if hasattr(result, 'stdout') else 'not_run'
            )
            return state

        except Exception as err:
            _LOGGER.error(
                "Error checking disk state for %s: %s",
                device_path if 'device_path' in locals() else device,
                err,
                exc_info=True
            )
            return DiskState.UNKNOWN

    async def update_spindown_delays(self) -> None:
        """Update disk spin-down delay settings."""
        try:
            result = await self._instance.execute_command("cat /boot/config/disk.cfg")
            if result.exit_status != 0:
                return

            for line in result.stdout.splitlines():
                if line.startswith("spindownDelay="):
                    delay = int(line.split("=")[1].strip('"'))
                    self._spindown_delays["default"] = delay * 60
                elif line.startswith("diskSpindownDelay."):
                    disk_num = line.split(".")[1].split("=")[0]
                    delay = int(line.split("=")[1].strip('"'))
                    if delay >= 0:
                        self._spindown_delays[f"disk{disk_num}"] = delay * 60

        except Exception as err:
            _LOGGER.error("Error updating spin-down delays: %s", err)

    def get_spindown_delay(self, disk_name: str) -> int:
        """Get spin-down delay for disk in seconds."""
        return self._spindown_delays.get(
            disk_name, 
            self._spindown_delays.get("default", 1800)
        )