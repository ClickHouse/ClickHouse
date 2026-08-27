#!/usr/bin/env python3
"""executable UDF that does CPU work per row and exits with code 3 after EOF.

Used to validate that with `check_exit_code=false` the source reaps via the
non-blocking, no-status-check path (`tryReapWithoutStatusCheck`), so:
  - a non-zero child exit does not raise an exception, and
  - rusage is still captured (`ExecutableUserDefinedFunctionUserTimeMicroseconds > 0`),
  - and `CHILD_WAS_NOT_EXITED_NORMALLY` is NOT logged.
"""

import os
import sys


def _cpu_work(seed: int) -> int:
    acc = 0
    base = seed & 0xFFFF
    for _ in range(300_000):
        acc = (acc + base) % 1000003
    return acc


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        n = int(line)
    except ValueError:
        n = 0
    sys.stdout.write(f"{_cpu_work(n)}\n")
    sys.stdout.flush()

# Flush before exiting so all output is visible before the process dies.
sys.stdout.flush()
os._exit(3)
