#!/usr/bin/python3

# A pooled command that answers each key and writes a diagnostic to `stderr` - on the pipe before
# the row is flushed, so it is already there whenever the server has its row.

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        updated_line = line.replace("\n", "")
        sys.stdout.write(updated_line + "\t" + "Key " + updated_line + "\n")

        sys.stderr.write("the command complains\n")
        sys.stderr.flush()

        sys.stdout.flush()
