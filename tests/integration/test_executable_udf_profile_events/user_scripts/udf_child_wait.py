#!/usr/bin/env python3
"""executable UDF that forks a CPU-burning child and waits for it.

Used to validate that CPU accumulated by a reaped child rolls up into
`ExecutableUserDefinedFunctionUserTimeMicroseconds` via wait4 cutime/cstime.
The parent does negligible CPU; the burn happens entirely in the child.
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
        # Child: burn CPU then exit without touching shared state.
        _burn_cpu()
        os._exit(0)
    else:
        # Parent: reap the child so its rusage rolls up into cutime/cstime.
        os.waitpid(pid, 0)
        sys.stdout.write(f"{n}\n")
        sys.stdout.flush()
