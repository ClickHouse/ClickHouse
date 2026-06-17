#!/usr/bin/env python3
"""executable UDF that forks a CPU-burning child but never waits for it.

Used to validate that CPU from an unreaped child is NOT attributed to the
parent via `ExecutableUserDefinedFunctionUserTimeMicroseconds`. The child's
rusage never rolls up into cutime/cstime because the parent never calls waitpid.
"""

import os
import sys


def _burn_cpu():
    x = 0
    for _ in range(24_000_000):
        x += 1


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        n = int(line)
    except ValueError:
        n = 0

    pid = os.fork()
    if pid == 0:
        # Child: detach from the parent's session so it cannot be accidentally
        # reaped, burn CPU, then exit.
        os.setsid()
        _burn_cpu()
        os._exit(0)
    else:
        # Parent: intentionally does NOT wait — the point is that the child's
        # CPU is excluded from the parent's rusage cutime/cstime.
        sys.stdout.write(f"{n}\n")
        sys.stdout.flush()
