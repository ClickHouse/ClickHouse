#!/usr/bin/python3

import sys
import re

if __name__ == "__main__":
    for line in sys.stdin:
        line_split = re.split(r"\t+", line)
        print(int(line_split[0]) + int(line_split[1]), end="\n")
        sys.stdout.flush()
