#!/usr/bin/python3

import sys
import os

if __name__ == '__main__':
    fd3 = os.fdopen(3)
    fd4 = os.fdopen(4)

    for chunk_header in fd4:
        fd4_chunk_length = int(chunk_header)
        print(str(fd4_chunk_length), end='\n')

        while fd4_chunk_length != 0:
            line = sys.stdin.readline()
            fd4_chunk_length -= 1
            print("Key from fd 4 " + line, end='')

        sys.stdout.flush()

        for chunk_header in fd3:
            fd3_chunk_length = int(chunk_header)
            print(str(fd3_chunk_length), end='\n')

            while fd3_chunk_length != 0:
                line = sys.stdin.readline()
                fd3_chunk_length -= 1
                print("Key from fd 3 " + line, end='')

            sys.stdout.flush()

            for chunk_header in sys.stdin:
                chunk_length = int(chunk_header)
                print(str(chunk_length), end='\n')

                while chunk_length != 0:
                    line = sys.stdin.readline()
                    chunk_length -= 1
                    print("Key " + line, end='')

                sys.stdout.flush()

#!/usr/bin/python3

import sys
import os

if __name__ == '__main__':
    fd3 = os.fdopen(3)
    fd4 = os.fdopen(4)

    for line in fd4:
        print("Key from 4 fd " + line, end='')

    for line in fd3:
        print("Key from 3 fd " + line, end='')

    for line in sys.stdin:
        print("Key from 0 fd " + line, end='')

    sys.stdout.flush()
