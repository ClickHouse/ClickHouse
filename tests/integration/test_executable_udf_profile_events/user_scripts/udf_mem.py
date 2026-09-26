#!/usr/bin/env python3
"""executable UDF that allocates a sizeable bytearray per call.

Used to validate PeakMemoryByteSeconds. Allocates ~32 MB on the first row
and frees it before responding; VmHWM observes that peak.
"""

import sys


def waste_memory() -> int:
    # ~32 MB allocation. Held briefly so VmHWM rises above the resting set.
    buf = bytearray(32 * 1024 * 1024)
    # Touch every page so the kernel actually maps the pages and VmHWM rises.
    for i in range(0, len(buf), 4096):
        buf[i] = (i & 0xFF)
    return len(buf)


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        n = int(line)
    except ValueError:
        n = 0
    used = waste_memory()
    sys.stdout.write(f"{(n + used) & 0xFFFFFFFFFFFFFFFF}\n")
    sys.stdout.flush()
