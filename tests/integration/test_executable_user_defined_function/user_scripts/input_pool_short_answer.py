#!/usr/bin/python3

# A pooled command that answers the first row only, closes its stdout, and then reads its stdin
# to the end before exiting normally - the way a pooled command is written to exit. It answered
# short, so it cannot go back to the pool; whether the server lets it see the end of its stdin is
# the difference between an exit within a moment and a `command_termination_timeout` sat out.

import os
import sys

if __name__ == "__main__":
    line = sys.stdin.readline()
    if line:
        os.write(1, ("Key " + line.strip() + "\n").encode())
    os.close(1)

    for _ in sys.stdin:
        pass
    sys.exit(0)
