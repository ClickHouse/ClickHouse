#!/usr/bin/env python3
"""executable_pool UDF worker whose memory lives in a *descendant*: it forks a
child at startup that allocates ~128 MiB, while the worker allocates nothing
big. The child exits when the worker dies (it polls for re-parenting)."""

import os
import sys
import time

CHILD_ALLOC_BYTES = 128 * 1024 * 1024

_parent_pid = os.getpid()
_child_pid = os.fork()
if _child_pid == 0:
    # Drop the inherited stdio pipe ends of the worker's protocol.
    os.close(0)
    os.close(1)
    ballast = bytearray(CHILD_ALLOC_BYTES)
    for i in range(0, len(ballast), 4096):
        ballast[i] = i & 0xFF
    # Exit when the worker dies: re-parenting changes our ppid.
    while os.getppid() == _parent_pid:
        time.sleep(0.5)
    os._exit(0)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    sys.stdout.write(f"{int(line)}\n")
    sys.stdout.flush()
