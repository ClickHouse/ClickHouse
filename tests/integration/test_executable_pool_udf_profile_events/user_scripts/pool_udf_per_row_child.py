#!/usr/bin/env python3
"""Scenario (c): per-row short-lived child, reaped before the next row.

For each input row, spawn a brand-new subprocess that runs a CPU loop,
exits, and is `wait`-ed by the parent. Every child has finished by the
time `recordReleased` runs, so the only place its CPU still exists is
in the parent's `cutime`. Used to validate that `cutime`/`cstime` plus
the Commit 2 full-`pre_snapshot` subtraction correctly attributes
per-row reaped CPU.
"""

import subprocess
import sys


_SNIPPET = (
    "import sys\n"
    "n = int(sys.argv[1])\n"
    "acc = 0\n"
    "for i in range(20000):\n"
    "    acc = (acc + n * i) % 1000003\n"
    "print(acc)\n"
)


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    n = int(line)
    child = subprocess.run(
        [sys.executable, "-c", _SNIPPET, str(n)],
        capture_output=True,
        text=True,
    )
    sys.stdout.write(child.stdout)
    sys.stdout.flush()
