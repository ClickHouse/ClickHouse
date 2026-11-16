#!/usr/bin/env python3
"""executable_pool UDF worker whose memory lives two generations down: it
forks a child, which forks a grandchild that allocates ~128 MiB. Each level
exits when its parent dies."""

import os
import sys
import time

GRANDCHILD_ALLOC_BYTES = 128 * 1024 * 1024


def follow_parent(parent_pid):
    while os.getppid() == parent_pid:
        time.sleep(0.5)
    os._exit(0)


_worker_pid = os.getpid()
if os.fork() == 0:
    # Child: drop the inherited stdio pipe ends of the worker's protocol
    # (only here; the grandchild inherits them already closed).
    os.close(0)
    os.close(1)
    _child_pid = os.getpid()
    if os.fork() == 0:
        ballast = bytearray(GRANDCHILD_ALLOC_BYTES)
        for i in range(0, len(ballast), 4096):
            ballast[i] = i & 0xFF
        follow_parent(_child_pid)
    follow_parent(_worker_pid)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    sys.stdout.write(f"{int(line)}\n")
    sys.stdout.flush()
