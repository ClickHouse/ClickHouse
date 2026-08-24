#!/usr/bin/python3

import sys

if __name__ == "__main__":
    arg = int(sys.argv[1])

    for line in sys.stdin:
        updated_line = line.replace("\n", "")
        print(updated_line + "\t" + "Key " + str(arg) + " " + updated_line, end="\n")
        sys.stdout.flush()
