#!/usr/bin/env python3
"""executable_pool UDF that burns CPU, touches memory, and then answers one row short.

The short answer is what makes the borrow end badly. The command answers all but the last row of
the block and exits, so the server sees end-of-output with fewer rows than it asked for: a worker
that has lost track of the protocol like that cannot go back into the pool, the teardown reaps it,
and the query fails with a row-count mismatch.

The borrow's CPU and peak resident set are read out of `/proc/<pid>`, so they have to be read
before that reap. Afterwards there is no `/proc/<pid>` left and the invocation reports as though
the command had done no work at all - on exactly the calls whose accounting is most worth having.

The two halves are lost at different moments, which is why this script does two things rather than
one. CPU survives the process becoming a zombie, so it is only lost at the reap. `VmHWM` does not -
it goes the moment the process exits. So the command closes its stdout, which is what makes the
server see a short answer, and then lingers instead of exiting, long enough to be sampled while it
still has an `mm` to read. It exits well inside `command_termination_timeout`, so the wait that
follows still reaps it normally.

The block size is fixed by the test that uses this (2000 rows in one block), so answering 1999 of
them is the whole contract here.
"""

import os
import sys
import time

ROWS_TO_ANSWER = 1999

# Large enough to stand out against the interpreter's own footprint, and touched rather than merely
# allocated, so that it really lands in `VmHWM`.
BALLAST_BYTES = 64 * 1024 * 1024

# Long enough for the server to sample this process, short enough to stay well inside
# `command_termination_timeout`.
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

    sys.stdout.write(f"{cpu_work(n)}\n")
    sys.stdout.flush()

    answered += 1
    if answered == ROWS_TO_ANSWER:
        # One row short, and no more conversation: the server reads end-of-output rather than
        # waiting out `command_read_timeout` for a row that is not coming.
        os.close(1)

        # Still here, so there is still a `VmHWM` to read. `ballast` is kept referenced on purpose.
        assert ballast[0] == 1
        time.sleep(LINGER_SECONDS)
        break
