#!/usr/bin/python3

# A pooled command that answers in full, closes its stdout, and then reads its stdin to the end
# before exiting normally. It cannot go back to the pool without a stdout, so its exit code is
# read right after its answer; whether the server lets it see the end of its stdin first is the
# difference between an exit within a moment and a `command_termination_timeout` sat out.

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
