#!/usr/bin/env python3
"""executable_pool UDF that does CPU work per row.

Used to validate UserTimeMicroseconds is non-zero. Per-row CPU is sized so
that a 1000-row block accumulates well above 10 ms — the tick granularity of
the 100 Hz clock that drives `/proc/<pid>/stat` utime/stime.
"""

import sys


def cpu_work(seed: int) -> int:
    acc = 0
    base = seed & 0xFFFF
    for i in range(3000):
        acc = (acc + (base + i) * (base + i)) % 1000003
    return acc


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        n = int(line)
    except ValueError:
        n = 0
    sys.stdout.write(f"{cpu_work(n)}\n")
    sys.stdout.flush()
