#!/usr/bin/python3

# A pooled pipe-mode command that answers correctly, goes quiet long enough to be handed back to the
# pool, and only then writes an extra row to its `stdout`.
#
# The gap is what makes it different from `input_pool_chatty.py`: the probe when the worker is
# handed back finds an empty pipe and cannot say anything about what comes next. That leaves the
# byte waiting for whoever borrows this process next - and the pipe transport has no framing that
# would let that query tell a stale row from its own. Parsed as its first row, it is a silently
# wrong answer, which is exactly what a borrow must refuse to start on.

import os
import sys
import time

QUIET_GAP_SECONDS = 0.3

if __name__ == "__main__":
    for line in sys.stdin:
        if not line.strip():
            continue

        sys.stdout.write(f"{os.getpid()}\n")
        sys.stdout.flush()

        # Long enough that the worker is back in the pool before this lands.
        time.sleep(QUIET_GAP_SECONDS)
        sys.stdout.write("999999\n")
        sys.stdout.flush()
