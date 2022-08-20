#!/usr/bin/python3

import sys
import re

if __name__ == "__main__":
    for chunk_header in sys.stdin:
        chunk_length = int(chunk_header)
        print(str(chunk_length), end="\n")

        while chunk_length != 0:
            line = sys.stdin.readline()
            line_split = re.split(r"\t+", line)
            print(int(line_split[0]) + int(line_split[1]), end="\n")
            chunk_length -= 1

        sys.stdout.flush()
