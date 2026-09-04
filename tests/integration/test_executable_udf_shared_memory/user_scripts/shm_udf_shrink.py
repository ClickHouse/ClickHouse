#!/usr/bin/python3

# Misbehaving executable UDF: it truncates the shared-memory file before answering, which is what an
# `O_TRUNC` in a client implementation does by accident. The server keeps mapping the extent the
# file no longer backs, so touching those pages would raise `SIGBUS` and take the whole server down
# — the response below deliberately points at an offset the server still believes to be valid. The
# server must detect the resize, fail the query and stay alive.

import os
import sys

PROTOCOL_VERSION = 1
STATUS_OK = 0


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
            break  # stdin closed -> exit

        path_length = read_varint(stdin)
        path = stdin.read(path_length).decode("utf-8")
        read_varint(stdin)  # input offset
        read_varint(stdin)  # input size

        fd = os.open(path, os.O_RDWR)
        try:
            os.ftruncate(fd, 0)
        finally:
            os.close(fd)

        # Well inside the region size the server still has: only comparing the file against that
        # size can catch this.
        write_varint(stdout, STATUS_OK)
        write_varint(stdout, 0)
        write_varint(stdout, 2)
        stdout.flush()


if __name__ == "__main__":
    main()
