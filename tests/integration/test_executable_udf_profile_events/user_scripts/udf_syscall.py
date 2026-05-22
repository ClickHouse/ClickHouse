#!/usr/bin/env python3
"""executable UDF that burns kernel-mode CPU via syscalls.

Used to validate `ExecutableUserDefinedFunctionSystemTimeMicroseconds`.
Each iteration issues a zero-byte `write(2)` to stdout: a real syscall
that the kernel still has to process but that emits no bytes, so the
TabSeparated UInt64 row written at the end of the per-row loop stays
well-formed. Sandbox-safe: no filesystem access.
"""

import os
import sys


# Tuned so that one row accumulates well above 10 ms of kernel CPU
# (Linux's 100 Hz clock-tick granularity). Each iteration is one
# `write(2)` syscall.
SYSCALLS_PER_ROW = 50000


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        n = int(line)
    except ValueError:
        n = 0

    for _ in range(SYSCALLS_PER_ROW):
        os.write(1, b"")

    sys.stdout.write(f"{n}\n")
    sys.stdout.flush()
