"""Helpers to inspect a ClickHouse binary.

Kept free of third-party imports so the CI job can use it without pulling in the
whole Dolor runtime.
"""

import mmap


def detect_private_binary(binary_path: str) -> bool:
    """Detect a private build by a symbol only it exports."""
    with open(binary_path, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            return mm.find(b"isCoordinatedMergesTasksActivated") > -1
        finally:
            mm.close()
