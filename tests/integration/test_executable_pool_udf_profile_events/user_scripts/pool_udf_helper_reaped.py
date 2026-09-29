#!/usr/bin/env python3
"""Scenario (b): persistent helper reaped mid-borrow.

Spawns a helper subprocess at script startup. After a small number of
rows the helper is closed and `wait`-ed, so its full lifetime CPU lands
in the parent's `cutime`/`cstime`. Subsequent rows are computed by the
parent itself. Used to validate that reaped descendant CPU is captured
via `cutime`/`cstime` plus the Commit 2 full-`pre_snapshot` subtraction.
"""

import subprocess
import sys


_helper = subprocess.Popen(
    [
        sys.executable,
        "-u",
        "-c",
        (
            "import sys\n"
            "for line in sys.stdin:\n"
            "    line = line.strip()\n"
            "    if not line:\n"
            "        continue\n"
            "    n = int(line)\n"
            "    acc = 0\n"
            "    for i in range(100000):\n"
            "        acc = (acc + n * i) % 1000003\n"
            "    sys.stdout.write(f'{acc}\\n')\n"
            "    sys.stdout.flush()\n"
        ),
    ],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    text=True,
    bufsize=1,
)

ROWS_BEFORE_REAP = 3
_row_count = 0


def _parent_work(value: int) -> int:
    acc = 0
    for i in range(100000):
        acc = (acc + value * i) % 1000003
    return acc


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    n = int(line)
    _row_count += 1

    if _helper is not None and _row_count <= ROWS_BEFORE_REAP:
        _helper.stdin.write(line + "\n")
        _helper.stdin.flush()
        sys.stdout.write(_helper.stdout.readline())
    else:
        if _helper is not None:
            _helper.stdin.close()
            _helper.wait()
            _helper = None
        sys.stdout.write(f"{_parent_work(n)}\n")
    sys.stdout.flush()
