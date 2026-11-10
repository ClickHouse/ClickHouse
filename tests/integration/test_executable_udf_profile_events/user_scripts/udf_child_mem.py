#!/usr/bin/env python3
"""executable UDF that forks one memory-hungry child held alive at its peak.

Validates that the /proc VmHWM subtree sampler captures a child process's peak
RSS. The child allocates ~64 MiB, signals readiness, and stays blocked (alive at
its peak) until the parent has produced output, so the sampler — which runs on
stdout reads, throttled to ~5 ms — observes the child while it holds its peak.
The per-row pause widens that observation window; it is scaffolding for a
sampling metric, not a workaround for a logic race.
"""
import os
import sys
import time

ready_r, ready_w = os.pipe()
go_r, go_w = os.pipe()
pid = os.fork()
if pid == 0:
    os.close(ready_r)
    os.close(go_w)
    buf = bytearray(64 * 1024 * 1024)
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
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        n = int(line)
    except ValueError:
        n = 0
    sys.stdout.write(f"{n}\n")
    sys.stdout.flush()
    time.sleep(0.02)
os.write(go_w, b"x")
os.close(go_w)
os.waitpid(pid, 0)
