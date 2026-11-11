#!/usr/bin/env python3
"""Scenario (a): persistent helper alive throughout the borrow.

Spawns one long-lived helper subprocess at script startup that does all
the CPU work. The helper stays alive across rows and across borrows in
the same pool slot. Used to validate that the sampler attributes CPU to
a persistent descendant via the post-walk per-pid delta.
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
            "    for i in range(50000):\n"
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


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    _helper.stdin.write(line + "\n")
    _helper.stdin.flush()
    sys.stdout.write(_helper.stdout.readline())
    sys.stdout.flush()
