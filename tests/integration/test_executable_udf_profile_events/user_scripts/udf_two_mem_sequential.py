#!/usr/bin/env python3
"""executable UDF that forks two memory-hungry children strictly sequentially.

Reference case for max-not-sum: each child allocates ~100 MiB, but the first is
fully reaped before the second is forked, so they never coexist. The sampler
observes at most one ~100 MiB child at a time, so the reported peak is ~100 MiB
— the same order as the concurrent case, confirming the metric takes the per-pid
maximum rather than summing. Each child is held alive at its peak across a brief
window so a sample observes it.
"""
import os
import sys
import time


def spawn_and_hold():
    ready_r, ready_w = os.pipe()
    go_r, go_w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(ready_r)
        os.close(go_w)
        buf = bytearray(100 * 1024 * 1024)
        for i in range(0, len(buf), 4096):
            buf[i] = i & 0xFF
        os.write(ready_w, b"x")
        os.close(ready_w)
        os.read(go_r, 1)
        os.close(go_r)
        os._exit(0)
    os.close(ready_w)
    os.close(go_r)
    os.read(ready_r, 1)
    os.close(ready_r)
    return pid, go_w


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        n = int(line)
    except ValueError:
        n = 0

    pid1, go_w1 = spawn_and_hold()
    sys.stdout.write(f"{n}\n")
    sys.stdout.flush()
    time.sleep(0.03)
    os.write(go_w1, b"x")
    os.close(go_w1)
    os.waitpid(pid1, 0)

    pid2, go_w2 = spawn_and_hold()
    time.sleep(0.03)
    os.write(go_w2, b"x")
    os.close(go_w2)
    os.waitpid(pid2, 0)
