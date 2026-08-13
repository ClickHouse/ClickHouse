#!/usr/bin/env python3
"""executable UDF that does CPU work per row and exits with code 3 after EOF.

Validates that with `check_exit_code=false` the source takes the no-status-check
wait path (`tryWaitWithoutStatusCheck`): a non-zero exit raises nothing, rusage is
still captured (`ExecutableUserDefinedFunctionUserTimeMicroseconds > 0`), and
`CHILD_WAS_NOT_EXITED_NORMALLY` is not logged.
"""

import os
import sys
import time


def _cpu_work(seed: int) -> int:
    acc = 0
    base = seed & 0xFFFF
    for _ in range(300_000):
        acc = (acc + base) % 1000003
    return acc


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        n = int(line)
    except ValueError:
        n = 0
    sys.stdout.write(f"{_cpu_work(n)}\n")
    sys.stdout.flush()

# Close stdout so ClickHouse sees EOF and starts cleanup while this process is still
# alive, then sleep so it is provably still running (not yet a zombie) when the wait
# runs. A single wait4(WNOHANG) then returns 0 and loses the rusage; the wait must
# poll to still capture it. The 2s delay sits inside command_termination_timeout (5s)
# but above a 1s window, so a budget that follows command_termination_timeout captures
# this child's rusage in cleanup while a fixed 1s budget would miss it.
sys.stdout.flush()
os.close(1)
time.sleep(2.0)
os._exit(3)
