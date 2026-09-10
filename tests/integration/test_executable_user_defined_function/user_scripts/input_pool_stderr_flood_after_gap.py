#!/usr/bin/python3

# The pipe-mode twin of `shm_udf_stderr_flood_after_gap.py`: a pooled command that answers, goes
# quiet for longer than the server's hand-back drain, and only then writes more to `stderr` than a
# pipe can hold.
#
# Configured with `stderr_reaction` `none`, which promises that a chatty command never blocks on a
# full `stderr` pipe. The quiet gap defeats any check that only looks at the pipe at the moment the
# worker is handed back; what keeps the promise is that the read loop of the next borrow polls
# `stderr` alongside `stdout`, so the query waiting for a response is the one that unblocks the
# command writing it.
#
# Answers with its own pid, so a reused worker is visible as a repeated one.

import os
import sys
import time

CHATTER = "e" * (128 * 1024)
QUIET_GAP_SECONDS = 0.3

if __name__ == "__main__":
    for line in sys.stdin:
        if not line.strip():
            continue

        sys.stdout.write(f"{os.getpid()}\n")
        sys.stdout.flush()

        time.sleep(QUIET_GAP_SECONDS)
        sys.stderr.write(CHATTER)
        sys.stderr.flush()
