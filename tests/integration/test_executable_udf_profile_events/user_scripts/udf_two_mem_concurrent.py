#!/usr/bin/env python3
"""executable UDF that forks two memory-hungry children that coexist simultaneously.

Used to validate T5: that `/proc VmHWM` sampling reports max-of-peaks, not the
concurrent sum. Each child allocates ~100 MiB and stays alive while the parent
writes its output row, so both are visible in the sampler's subtree walk at that
moment (~200 MiB live simultaneously). The metric reports ~100 MiB (the per-pid
max), not ~200 MiB (the concurrent aggregate), confirming max-not-sum semantics.
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

    # Signalling pipes created before forking so each child shares the same fds.
    r1, w1 = os.pipe()
    r2, w2 = os.pipe()

    pid1 = os.fork()
    if pid1 == 0:
        # Child 1: close unused ends; allocate ~100 MiB, touch every page so
        # VmHWM rises; block until parent signals after writing output.
        os.close(w1)
        os.close(r2)
        os.close(w2)
        buf = bytearray(100 * 1024 * 1024)
        for i in range(0, len(buf), 4096):
            buf[i] = i & 0xFF
        os.read(r1, 1)
        os.close(r1)
        os._exit(0)

    pid2 = os.fork()
    if pid2 == 0:
        # Child 2: close unused ends; allocate ~100 MiB, touch every page so
        # VmHWM rises; block until parent signals after writing output.
        os.close(r1)
        os.close(w1)
        os.close(w2)
        buf = bytearray(100 * 1024 * 1024)
        for i in range(0, len(buf), 4096):
            buf[i] = i & 0xFF
        os.read(r2, 1)
        os.close(r2)
        os._exit(0)

    # Parent: close the child read ends it no longer needs; write output while
    # both children are alive so the sampler's stdout-IO subtree walk sees them;
    # then signal each child and reap both.
    os.close(r1)
    os.close(r2)
    sys.stdout.write(f"{n}\n")
    sys.stdout.flush()
    os.write(w1, b"x")
    os.close(w1)
    os.write(w2, b"x")
    os.close(w2)
    os.waitpid(pid1, 0)
    os.waitpid(pid2, 0)
