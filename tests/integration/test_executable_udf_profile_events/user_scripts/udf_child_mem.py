#!/usr/bin/env python3
"""executable UDF that forks a memory-hungry child and waits for it.

Used to validate that the peak RSS of the child subtree is captured via
`/proc/<pid>/VmHWM` sampled while the child is alive during IO.
The child allocates ~64 MiB and stays resident until the parent has written
its output row so the sampler observes the child's `VmHWM` during the
stdout-IO sample triggered by that write.
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

    # Create the signalling pipe before forking so both sides share the same fds.
    child_r, parent_w = os.pipe()

    pid = os.fork()
    if pid == 0:
        # Child: close the write end inherited from the parent; allocate ~64 MiB
        # and touch every page so VmHWM rises; then block on the read end until
        # the parent writes a byte after flushing output (keeping this process
        # alive for the sampler's subtree walk during that flush).
        os.close(parent_w)
        buf = bytearray(64 * 1024 * 1024)
        for i in range(0, len(buf), 4096):
            buf[i] = i & 0xFF
        os.read(child_r, 1)
        os.close(child_r)
        os._exit(0)
    else:
        # Parent: close the read end; write output so the sampler's stdout-IO
        # sample sees the child alive in the subtree walk; then signal the child
        # it may exit and reap it.
        os.close(child_r)
        sys.stdout.write(f"{n}\n")
        sys.stdout.flush()
        os.write(parent_w, b"x")
        os.close(parent_w)
        os.waitpid(pid, 0)
