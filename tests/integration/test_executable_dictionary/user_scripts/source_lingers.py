#!/usr/bin/python3

# A source that produces its rows, closes its stdout and then stays alive far longer than the
# `command_termination_timeout` it is configured with. Whether its exit code is wanted decides
# whether the load fails or the process is simply signalled once the budget is spent.

import os
import sys
import time

if __name__ == "__main__":
    print("1" + "\t" + "Value 1", end="\n")
    print("2" + "\t" + "Value 2", end="\n")
    print("3" + "\t" + "Value 3", end="\n")

    sys.stdout.flush()
    # `sys.stdout.close()` leaves the descriptor open (the standard streams are opened with
    # `closefd=False`); the server only sees EOF once the descriptor itself is closed.
    os.close(sys.stdout.fileno())

    time.sleep(60)
