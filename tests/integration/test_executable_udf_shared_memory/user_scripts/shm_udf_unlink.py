#!/usr/bin/python3

# Misbehaving executable UDF: it deletes the shared-memory file after mapping it, which is what a
# client "cleaning up after itself" does by accident. The server's descriptor keeps the file alive,
# so this very request still works; but the path is gone, and for a pooled worker every later borrow
# would hand the command a name that leads nowhere. The server must notice and discard the worker.

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
        request_id = read_varint(stdin)

        path_length = read_varint(stdin)
        path = stdin.read(path_length).decode("utf-8")
        read_varint(stdin)  # input offset
        read_varint(stdin)  # input size

        os.unlink(path)

        # A perfectly valid answer: only checking that the path still names this file catches it.
        write_varint(stdout, request_id)
        write_varint(stdout, STATUS_OK)
        write_varint(stdout, 0)
        write_varint(stdout, 2)
        stdout.flush()


if __name__ == "__main__":
    main()
