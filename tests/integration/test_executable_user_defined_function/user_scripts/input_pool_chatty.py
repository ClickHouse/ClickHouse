#!/usr/bin/python3

# A pooled pipe-mode command that answers correctly and writes one byte too many right after it.
#
# The byte arrives with the answer, so the server reads it into its own buffer along with the rows,
# and it dies with that buffer: it never reaches the pipe the next borrower reads, and the worker is
# still at a usable boundary. A byte written later, once the server has stopped reading, is the
# other case - see `input_pool_late_stdout.py`.
#
# It answers with its own pid, so the test can tell a fresh worker from a reused one.

import os
import sys

if __name__ == "__main__":
    for line in sys.stdin:
        if not line.strip():
            continue
        # The answer and one byte past it. A newline, so that a next borrow reading it would see a
        # plausible - and empty - first row rather than a parse error. In one write, so that the byte
        # is read into the server's buffer together with the row: flushed on its own, it could reach
        # the pipe only after the server finished reading, and whether the worker is kept would be
        # decided by the scheduler.
        sys.stdout.write(f"{os.getpid()}\n\n")
        sys.stdout.flush()
