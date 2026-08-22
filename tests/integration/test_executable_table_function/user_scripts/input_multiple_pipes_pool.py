#!/usr/bin/python3

import os
import sys

if __name__ == "__main__":
    fd3 = os.fdopen(3)
    fd4 = os.fdopen(4)

    lines = []

    for chunk_header_fd4 in fd4:
        fd4_chunk_length = int(chunk_header_fd4)

        while fd4_chunk_length != 0:
            line = fd4.readline()
            fd4_chunk_length -= 1
            lines.append("Key from 4 fd " + line)

        for chunk_header_fd3 in fd3:
            fd3_chunk_length = int(chunk_header_fd3)

            while fd3_chunk_length != 0:
                line = fd3.readline()
                fd3_chunk_length -= 1
                lines.append("Key from 3 fd " + line)

            for chunk_header in sys.stdin:
                chunk_length = int(chunk_header)

                while chunk_length != 0:
                    line = sys.stdin.readline()
                    chunk_length -= 1
                    lines.append("Key from 0 fd " + line)

                break
            break

        print(str(len(lines)), end="\n")

        for line in lines:
            print(line, end="")
        lines.clear()

        sys.stdout.flush()
