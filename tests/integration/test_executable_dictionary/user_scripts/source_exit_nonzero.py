#!/usr/bin/python3

# A source that produces its rows and then exits with a non-zero code.

import sys

if __name__ == "__main__":
    print("1" + "\t" + "Value 1", end="\n")
    print("2" + "\t" + "Value 2", end="\n")
    print("3" + "\t" + "Value 3", end="\n")

    sys.stdout.flush()
    sys.exit(3)
