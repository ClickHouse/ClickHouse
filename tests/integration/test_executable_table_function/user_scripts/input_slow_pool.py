#!/usr/bin/python3

import sys
import time

if __name__ == "__main__":
    for chunk_header in sys.stdin:
        time.sleep(25)

        chunk_length = int(chunk_header)
        print(str(chunk_length), end="\n")

        while chunk_length != 0:
            line = sys.stdin.readline()
            chunk_length -= 1
            print("Key " + line, end="")

        sys.stdout.flush()
