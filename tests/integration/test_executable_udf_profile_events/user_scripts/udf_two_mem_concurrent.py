#!/usr/bin/env python3
"""executable UDF that forks two memory-hungry children that coexist simultaneously.

Used to validate T5: that ru_maxrss reports max-of-peaks, not the concurrent sum.
Each child allocates ~100 MiB and holds it while the other is also resident,
so both live in memory at the same time (~200 MiB live). If the metric were a
true concurrent aggregate it would report ~200 MiB; ru_maxrss reports ~100 MiB.
"""

import os
import sys
import time


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        n = int(line)
    except ValueError:
        n = 0

    pid1 = os.fork()
    if pid1 == 0:
        # Child 1: allocate ~100 MiB, hold briefly, exit.
        buf = bytearray(100 * 1024 * 1024)
        for i in range(0, len(buf), 4096):
            buf[i] = i & 0xFF
        time.sleep(0.5)
        os._exit(0)

    pid2 = os.fork()
    if pid2 == 0:
        # Child 2: allocate ~100 MiB, hold briefly, exit.
        buf = bytearray(100 * 1024 * 1024)
        for i in range(0, len(buf), 4096):
            buf[i] = i & 0xFF
        time.sleep(0.5)
        os._exit(0)

    # Parent: reap both children (they coexist during the sleep).
    os.waitpid(pid1, 0)
    os.waitpid(pid2, 0)
    sys.stdout.write(f"{n}\n")
    sys.stdout.flush()
