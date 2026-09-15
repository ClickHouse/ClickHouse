#!/usr/bin/python3

# A pooled command that answers correctly, closes its stdout, and then refuses to leave: it sleeps
# far past `command_termination_timeout` and only then exits non-zero.
#
# A pooled worker is never waited for while it can still go back to the pool. One that closed its
# stdout cannot - it is gone as far as the protocol is concerned - so under `check_exit_code` its
# status is read then and there. This one does not have a status to read within the budget, and a
# status that could not be read is not a passing one: the query fails the same way a plain
# `executable` command that lingers fails it. The `sys.exit(1)` below is what nothing would see.
#
# The answer is written without its trailing newline: the row is then complete only once the
# server has read end-of-output, so the closed stdout is a fact by the time the rows are, and the
# check after the answer sees a hung-up worker every time rather than only when it happens to look
# after the close.

import os
import sys
import time

if __name__ == "__main__":
    for line in sys.stdin:
        os.write(1, ("Key " + line.strip()).encode())
        os.close(1)

        # Far past the function's `command_termination_timeout`; the SIGTERM that follows ends it.
        time.sleep(600)
        sys.exit(1)
