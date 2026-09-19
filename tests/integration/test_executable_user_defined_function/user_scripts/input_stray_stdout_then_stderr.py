#!/usr/bin/python3

# Answers its rows, then - after the server has had its answer - writes one stray line to stdout
# and only then its diagnostic to stderr, before exiting. The stray write must not be what kills
# the process: if the server closed its stdout as soon as it had the rows, that write would die on
# SIGPIPE with the diagnostic still unwritten, and `stderr_reaction` would never see it.

import os
import sys
import time

if __name__ == "__main__":
    for line in sys.stdin:
        if not line.strip():
            continue
        os.write(1, ("Key " + line.strip() + "\n").encode())
    time.sleep(0.3)
    os.write(1, b"stray line\n")
    os.write(2, b"late complaint\n")
    sys.exit(0)
