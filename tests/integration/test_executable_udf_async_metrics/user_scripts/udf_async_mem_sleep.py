#!/usr/bin/env python3
"""executable UDF that holds ~128 MiB while sleeping ~30 s per row,
so an in-flight invocation stays observable even on slow CI runners."""

import sys
import time

ALLOC_BYTES = 128 * 1024 * 1024
SLEEP_SECONDS = 30.0

_ballast = bytearray(ALLOC_BYTES)
# Touch every page so the kernel actually maps them and VmRSS rises.
for i in range(0, len(_ballast), 4096):
    _ballast[i] = i & 0xFF

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    time.sleep(SLEEP_SECONDS)
    sys.stdout.write(f"{int(line)}\n")
    sys.stdout.flush()
