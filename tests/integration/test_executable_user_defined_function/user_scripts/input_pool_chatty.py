#!/usr/bin/python3

# A pooled pipe-mode command that answers correctly and then writes one byte too many.
#
# The row count the server asked for is satisfied, so the borrow looks successful - and that is the
# whole problem. The extra byte stays on the pipe, and the next query to borrow this worker reads it
# as the beginning of *its* result: not a protocol error but corrupted rows, which nothing else
# would catch. A worker that is not provably at a clean boundary must not be handed on.
#
# It answers with its own pid, so the test can tell a fresh worker from a reused one.

import os
import sys

if __name__ == "__main__":
    for line in sys.stdin:
        if not line.strip():
            continue
        sys.stdout.write(f"{os.getpid()}\n")
        sys.stdout.flush()

        # One byte past the answer. A newline, so that a next borrow reading it would see a
        # plausible - and empty - first row rather than a parse error.
        sys.stdout.write("\n")
        sys.stdout.flush()
