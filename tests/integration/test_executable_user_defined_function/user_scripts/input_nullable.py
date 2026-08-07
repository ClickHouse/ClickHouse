#!/usr/bin/python3

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        if line == "\\N\n":
            print("Key Nullable", end="\n")
        else:
            print("Key " + line, end="")

        sys.stdout.flush()
