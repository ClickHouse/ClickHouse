#!/usr/bin/python3

import sys

if __name__ == "__main__":
    print(f"{'a' * (3 * 1024)}{'b' * (3 * 1024)}{'c' * (3 * 1024)}", file=sys.stderr)
    sys.stderr.flush()
    for line in sys.stdin:
        print("Key " + line, end="")
        sys.stdout.flush()
