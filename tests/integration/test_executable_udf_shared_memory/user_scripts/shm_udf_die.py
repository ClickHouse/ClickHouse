#!/usr/bin/python3

# Misbehaving UDF: reads a request and then exits without answering, simulating a crashed
# command. The server must surface an error rather than hang or return wrong results.

import sys


def read_varint(stream):
    result = 0
    shift = 0
    while True:
        chunk = stream.read(1)
        if not chunk:
            return None
        byte = chunk[0]
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result
        shift += 7


def main():
    stdin = sys.stdin.buffer

    version = read_varint(stdin)
    if version is None:
        return

    path_length = read_varint(stdin)
    stdin.read(path_length)  # path
    read_varint(stdin)  # input offset
    read_varint(stdin)  # input size

    # Die without writing a response.
    sys.exit(1)


if __name__ == "__main__":
    main()
