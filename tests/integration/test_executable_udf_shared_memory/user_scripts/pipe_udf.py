#!/usr/bin/python3

# An ordinary pipe-mode UDF, in a suite that is otherwise all shared memory.
#
# It is here so that the experimental gate can be shown to be about one transport rather than about
# executable UDFs in general: with the setting off, this one keeps working.

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        sys.stdout.write(f"Key {line}\n")
        sys.stdout.flush()
