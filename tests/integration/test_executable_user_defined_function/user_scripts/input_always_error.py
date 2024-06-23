#!/usr/bin/python3

import sys

if __name__ == "__main__":
    print("Fake error", file=sys.stderr)
    sys.stderr.flush()
    for line in sys.stdin:
        print("Key " + line, end="")
        sys.stdout.flush()
