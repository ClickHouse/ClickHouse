#!/usr/bin/python3

import sys
import os

if __name__ == "__main__":
    fd3 = os.fdopen(3)
    fd4 = os.fdopen(4)

    for line in fd4:
        print("Key from 4 fd " + line, end="")

    for line in fd3:
        print("Key from 3 fd " + line, end="")

    for line in sys.stdin:
        print("Key from 0 fd " + line, end="")

    sys.stdout.flush()
