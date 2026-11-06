#!/usr/bin/env python3
"""executable_pool UDF worker that allocates ~128 MiB at startup and keeps it
resident for its lifetime, including while idling in the pool."""

import sys

ALLOC_BYTES = 128 * 1024 * 1024

_ballast = bytearray(ALLOC_BYTES)
# Touch every page so the kernel actually maps them and VmRSS rises.
for i in range(0, len(_ballast), 4096):
    _ballast[i] = i & 0xFF

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    sys.stdout.write(f"{int(line)}\n")
    sys.stdout.flush()
