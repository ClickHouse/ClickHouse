#!/usr/bin/env python3
"""executable_pool UDF that does CPU work per row and then answers one row short.

The short answer is what makes the borrow end badly. The command answers all but the last row of
the block and exits, so the server sees end-of-output with fewer rows than it asked for: a worker
that has lost track of the protocol like that cannot go back into the pool, the teardown reaps it,
and the query fails with a row-count mismatch.

The borrow's CPU and peak resident set are read out of `/proc/<pid>`, so they have to be read
before that reap. Afterwards there is no `/proc/<pid>` left and the invocation reports as though
the command had done no work at all - on exactly the calls whose accounting is most worth having.

The block size is fixed by the test that uses this (2000 rows in one block), so answering 1999 of
them is the whole contract here.
"""

import sys

ROWS_TO_ANSWER = 1999


def cpu_work(seed: int) -> int:
    acc = 0
    base = seed & 0xFFFF
    for i in range(3000):
        acc = (acc + (base + i) * (base + i)) % 1000003
    return acc


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
        break
