#!/usr/bin/python3

# A pooled command that answers correctly, goes quiet long enough to be handed back to the pool,
# and then exits non-zero while it sits there.
#
# Nothing is waiting for a pooled process between borrows, so its exit is seen by nobody until the
# next query borrows it. What that query must not get is a failure of its own on the first write to
# a closed stdin, for something that happened before it started: the dead process has to be
# replaced before the borrow is built on it. It answers with its own pid, so a replacement is
# visible as a different one.

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

        time.sleep(QUIET_GAP_SECONDS)
        sys.exit(1)
