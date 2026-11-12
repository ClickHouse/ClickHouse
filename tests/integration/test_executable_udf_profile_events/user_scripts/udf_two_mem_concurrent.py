#!/usr/bin/env python3
"""executable UDF that forks two memory-hungry children that coexist.

Validates max-not-sum: two children each allocate ~100 MiB and are alive
simultaneously while the parent produces output, so the sampler's subtree walk
sees both at once (~200 MiB resident together). The metric reports the per-pid
maximum (~100 MiB), not the sum (~200 MiB). Each child signals readiness and
stays blocked at its peak until output is done; the per-row pause widens the
sampler's observation window.
"""
import os
import sys
import time

ready_r, ready_w = os.pipe()
go_r, go_w = os.pipe()
pids = []
for _ in range(2):
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
    pids.append(pid)

os.close(ready_w)
os.close(go_r)
got = b""
while len(got) < 2:
    got += os.read(ready_r, 2 - len(got))
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
os.write(go_w, b"xx")
os.close(go_w)
for pid in pids:
    os.waitpid(pid, 0)
