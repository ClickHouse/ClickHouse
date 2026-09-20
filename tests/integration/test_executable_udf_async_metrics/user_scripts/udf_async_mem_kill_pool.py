#!/usr/bin/env python3
"""Same as udf_async_mem_pool.py, under a distinct file name so the kill test
can pkill this worker without touching the other tests' workers."""

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
