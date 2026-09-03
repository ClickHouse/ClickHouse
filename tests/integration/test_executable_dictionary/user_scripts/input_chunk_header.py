#!/usr/bin/python3

import sys

if __name__ == "__main__":
    for chunk_header in sys.stdin:
        chunk_length = int(chunk_header)

        while chunk_length != 0:
            line = sys.stdin.readline()
            updated_line = line.replace("\n", "")
            chunk_length -= 1
            print(updated_line + "\t" + "Key " + updated_line, end="\n")

        sys.stdout.flush()
