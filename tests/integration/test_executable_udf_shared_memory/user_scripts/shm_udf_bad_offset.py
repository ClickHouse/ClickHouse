#!/usr/bin/python3

# Misbehaving UDF: reads a request and reports success with an out-of-bounds output region.
# The server must reject the response instead of reading past the shared-memory region.

import os
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


def write_varint(stream, value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            break
    stream.write(bytes(out))


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    while True:
        version = read_varint(stdin)
        if version is None:
            break

        path_length = read_varint(stdin)
        path = stdin.read(path_length).decode("utf-8")
        read_varint(stdin)  # input offset
        read_varint(stdin)  # input size

        region_size = os.path.getsize(path)

        # Success status, but the output claims to live past the end of the region.
        write_varint(stdout, 0)
        write_varint(stdout, region_size + 1024)  # bogus offset
        write_varint(stdout, 16)  # bogus size
        stdout.flush()


if __name__ == "__main__":
    main()
