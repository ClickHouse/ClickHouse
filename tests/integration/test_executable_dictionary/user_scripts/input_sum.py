#!/usr/bin/python3

import sys
import re

if __name__ == "__main__":
    for line in sys.stdin:
        updated_line = line.replace("\n", "")
        line_split = re.split(r"\t+", line)
        sum = int(line_split[0]) + int(line_split[1])
        print(updated_line + "\t" + str(sum), end="\n")
        sys.stdout.flush()
