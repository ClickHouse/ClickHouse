#!/usr/bin/python3

# Answers correctly and then refuses to leave: on stdin EOF - the server telling it to exit - it
# sleeps far past `command_termination_timeout` and only then exits non-zero.
#
# `check_exit_code` says the exit status is checked and a non-zero one fails the query. The server
# cannot read a status from a process that has not exited, and it will not wait for one
# indefinitely: `command_termination_timeout` is what it waits, after which the process is
# signalled. A status that could not be read is not a passing status - waving the query through
# would make the setting mean "checked, unless the command avoids being checked", which is the one
# command it most needs to hold for. The `sys.exit(1)` below is what nothing would ever see.

import os
import sys
import time

if __name__ == "__main__":
    for line in sys.stdin:
        print("Key " + line, end="")
        sys.stdout.flush()

    # End the output, so the server has the whole answer and is only waiting for this process to
    # go. Without this the server is still waiting for rows and hits `command_read_timeout`
    # instead, which is a different failure from the one this script is about.
    os.close(1)

    # Far past the function's `command_termination_timeout`; the SIGTERM that follows ends it.
    time.sleep(600)
    sys.exit(1)
