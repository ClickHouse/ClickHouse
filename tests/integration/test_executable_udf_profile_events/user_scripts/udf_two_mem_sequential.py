#!/usr/bin/env python3
"""executable UDF that forks two memory-hungry children strictly sequentially.

Used as the reference case for T5: each child allocates ~100 MiB but the two
never coexist in memory — child1 exits and is reaped before child2 is forked.
The reported peak should be ~100 MiB (one child's peak), same order as the
concurrent case, confirming that ru_maxrss is max-of-peaks not a sum.
"""

import os
import sys


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        n = int(line)
    except ValueError:
        n = 0

    # Child 1: allocate, exit.
    pid1 = os.fork()
    if pid1 == 0:
        buf = bytearray(100 * 1024 * 1024)
        for i in range(0, len(buf), 4096):
            buf[i] = i & 0xFF
        os._exit(0)
    os.waitpid(pid1, 0)

    # Child 2: allocate, exit. Starts only after child 1 is fully reaped.
    pid2 = os.fork()
    if pid2 == 0:
        buf = bytearray(100 * 1024 * 1024)
        for i in range(0, len(buf), 4096):
            buf[i] = i & 0xFF
        os._exit(0)
    os.waitpid(pid2, 0)

    sys.stdout.write(f"{n}\n")
    sys.stdout.flush()
