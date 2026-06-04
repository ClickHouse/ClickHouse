#!/usr/bin/env python3
"""Scenario (d): multi-layer reaping chain (UDF → python helper → sort).

UDF script spawns a long-lived python helper. For each row the helper
spawns a `sort` subprocess on generated data, reads its output, reaps
it. The helper itself stays alive across rows and across borrows in the
same pool slot. Used to validate that `cutime`/`cstime` propagates one
level upward: the helper's `cutime` delta carries the CPU of every
`sort` grandchild it reaped.
"""

import subprocess
import sys


_HELPER_SNIPPET = (
    "import subprocess\n"
    "import sys\n"
    "for line in sys.stdin:\n"
    "    line = line.strip()\n"
    "    if not line:\n"
    "        continue\n"
    "    n = int(line)\n"
    "    data = '\\n'.join(str((n * i) % 1000003) for i in range(2000))\n"
    "    res = subprocess.run(['sort', '-n'], input=data, capture_output=True, text=True)\n"
    "    head = res.stdout.split('\\n', 1)[0] if res.stdout else '0'\n"
    "    sys.stdout.write(head + '\\n')\n"
    "    sys.stdout.flush()\n"
)


_helper = subprocess.Popen(
    [sys.executable, "-u", "-c", _HELPER_SNIPPET],
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
