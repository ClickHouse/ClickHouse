#!/usr/bin/env python3
"""executable UDF that forks two memory-hungry children strictly sequentially.

Used as the reference case for T5: the two children never coexist in memory —
child1 exits and is reaped before child2 is forked. Each child allocates ~100 MiB.
Child2 stays alive while the parent writes its output row so the sampler's
stdout-IO subtree walk captures child2's `VmHWM` (~100 MiB). The reported peak
is ~100 MiB, matching the concurrent case, which confirms max-not-sum semantics:
two non-overlapping 100 MiB peaks do not sum to 200 MiB.
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

    # Child 1: allocate and exit before child 2 is forked (strictly sequential).
    pid1 = os.fork()
    if pid1 == 0:
        buf = bytearray(100 * 1024 * 1024)
        for i in range(0, len(buf), 4096):
            buf[i] = i & 0xFF
        os._exit(0)
    os.waitpid(pid1, 0)

    # Create signalling pipe before forking child 2.
    r2, w2 = os.pipe()

    # Child 2: allocate ~100 MiB, touch every page so VmHWM rises; block until
    # the parent signals after writing output.
    pid2 = os.fork()
    if pid2 == 0:
        os.close(w2)
        buf = bytearray(100 * 1024 * 1024)
        for i in range(0, len(buf), 4096):
            buf[i] = i & 0xFF
        os.read(r2, 1)
        os.close(r2)
        os._exit(0)

    # Parent: close read end; write output while child 2 is alive so the sampler
    # sees its VmHWM; signal child 2 and reap it.
    os.close(r2)
    sys.stdout.write(f"{n}\n")
    sys.stdout.flush()
    os.write(w2, b"x")
    os.close(w2)
    os.waitpid(pid2, 0)
