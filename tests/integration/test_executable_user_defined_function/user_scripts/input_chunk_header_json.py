#!/usr/bin/python3

import sys
import json

if __name__ == "__main__":
    for chunk_header in sys.stdin:
        chunk_length = int(chunk_header)

        for _ in range(chunk_length):
            row = json.loads(sys.stdin.readline().strip())
            print(f'{{"result": "Key {row["number"]}"}}')

        sys.stdout.flush()
