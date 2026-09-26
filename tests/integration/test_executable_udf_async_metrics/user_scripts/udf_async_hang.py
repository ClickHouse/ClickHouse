#!/usr/bin/env python3
"""executable UDF that allocates ~128 MiB after reading input and then hangs
without ever writing output, so command_read_timeout fails the invocation."""

import sys
import time

ALLOC_BYTES = 128 * 1024 * 1024

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    _ballast = bytearray(ALLOC_BYTES)
    # Touch every page so the kernel actually maps them and VmRSS rises.
    for i in range(0, len(_ballast), 4096):
        _ballast[i] = i & 0xFF
    while True:
        time.sleep(1)
