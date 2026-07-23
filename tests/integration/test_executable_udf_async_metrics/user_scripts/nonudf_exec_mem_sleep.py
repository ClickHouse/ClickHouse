#!/usr/bin/env python3
"""Script for the Executable TABLE ENGINE (deliberately not a UDF): holds
~128 MiB while sleeping ~30 s, then emits one row. Touches a marker file once
the ballast is resident so the test knows when to look."""

import sys
import time

ALLOC_BYTES = 128 * 1024 * 1024
SLEEP_SECONDS = 30.0

_ballast = bytearray(ALLOC_BYTES)
# Touch every page so the kernel actually maps them and VmRSS rises.
for i in range(0, len(_ballast), 4096):
    _ballast[i] = i & 0xFF

with open("/tmp/nonudf_running", "w") as marker:
    marker.write("1")

time.sleep(SLEEP_SECONDS)
sys.stdout.write("1\n")
sys.stdout.flush()
