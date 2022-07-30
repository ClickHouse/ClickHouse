#!/usr/bin/python3

import sys

if __name__ == "__main__":
    arg = int(sys.argv[1])

    for line in sys.stdin:
        print("Key " + str(arg) + " " + line, end="")
        sys.stdout.flush()
