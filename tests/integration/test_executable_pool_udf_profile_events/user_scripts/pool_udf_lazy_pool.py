#!/usr/bin/env python3
"""Scenario (e): lazily-initialised multiprocessing.Pool.

The Pool is created on the FIRST input row of the first borrow, not at
script startup. Its worker processes therefore do not exist when the
sampler's pre-walk runs — they are spawned during the borrow. Used to
validate Commit 3's three-bucket dispatch: pids absent from
`pre_walk_pids` are counted with the full post value, so the lazy Pool
workers' CPU still shows up in `UserTimeMicroseconds`.
"""

import multiprocessing
import sys


def _work(n: int) -> int:
    acc = 0
    for i in range(80000):
        acc = (acc + n * i) % 1000003
    return acc


_pool = None


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    n = int(line)
    if _pool is None:
        _pool = multiprocessing.Pool(processes=2)
    sys.stdout.write(f"{_pool.apply(_work, (n,))}\n")
    sys.stdout.flush()
