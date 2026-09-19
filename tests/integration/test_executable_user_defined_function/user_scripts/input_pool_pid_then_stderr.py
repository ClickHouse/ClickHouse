#!/usr/bin/python3

# A pooled command that answers each row with its own pid and then writes a line to stderr - after
# the rows, so that the line lands on the pipe after the server has read its answer. Under a `log*`
# reaction that line is a log line, not a verdict, and must not cost the command its process: the
# pid in the answer is what tells one process from another across calls.

import os
import sys

if __name__ == "__main__":
    for line in sys.stdin:
        if not line.strip():
            continue
        sys.stdout.write(f"{os.getpid()}\n")
        sys.stdout.flush()
        sys.stderr.write("logging right after the rows\n")
        sys.stderr.flush()
