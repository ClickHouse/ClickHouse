#!/usr/bin/env python3
"""executable_pool UDF that burns CPU, touches memory, answers in full, and then exits.

The full answer is what makes this different from `pool_udf_cpu_short.py`: the query succeeds. But
the worker is still gone - it closes its stdout right after the last row - so it cannot go back to
the pool, and under `check_exit_code` its status is read there and then. That read reaps it, and
`/proc/<pid>` goes with it: the borrow's CPU and peak resident set have to be sampled before the
reap, or a command that did its work and left politely is reported as having done nothing.

Like the short variant, it lingers a little after closing its stdout rather than exiting at once,
so that there is still an `mm` - and a `VmHWM` - to read when the server samples it, and exits well
inside `command_termination_timeout`, so the wait that follows reaps it normally.

The last row is written without its trailing newline: the row is then complete only once the
server has read end-of-output, so the closed stdout is a fact by the time the rows are, and the
check after the answer sees a hung-up worker every time.

The block size is fixed by the test that uses this (2000 rows in one block).
"""

import os
import sys
import time

ROWS_TO_ANSWER = 2000

BALLAST_BYTES = 64 * 1024 * 1024

LINGER_SECONDS = 0.5


def cpu_work(seed: int) -> int:
    acc = 0
    base = seed & 0xFFFF
    for i in range(3000):
        acc = (acc + (base + i) * (base + i)) % 1000003
    return acc


ballast = bytearray(BALLAST_BYTES)
for offset in range(0, BALLAST_BYTES, 4096):
    ballast[offset] = 1

answered = 0
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        n = int(line)
    except ValueError:
        n = 0

    answered += 1
    if answered < ROWS_TO_ANSWER:
        sys.stdout.write(f"{cpu_work(n)}\n")
        sys.stdout.flush()
        continue

    sys.stdout.write(f"{cpu_work(n)}")
    sys.stdout.flush()
    os.close(1)

    # Still here, so there is still a `VmHWM` to read. `ballast` is kept referenced on purpose.
    assert ballast[0] == 1
    time.sleep(LINGER_SECONDS)
    break
