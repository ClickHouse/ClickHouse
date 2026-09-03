#!/usr/bin/python3

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        print("Key " + line, end="")
        sys.stdout.flush()

    sys.exit(1)
