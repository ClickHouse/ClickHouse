#!/usr/bin/python3

# Misbehaving executable UDF: it enlarges the shared-memory file and touches the new pages before
# answering. Nothing faults, and the answer itself is inside the region the server knows about, so
# only comparing the file against the mapping catches it: those pages are committed in the `tmpfs`
# and charged to nobody, and for a pooled worker the trim on the way back to the pool would not free
# them. The server must detect the resize and fail the query.

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

        fd = os.open(path, os.O_RDWR)
        try:
            os.ftruncate(fd, 64 * 1024 * 1024)
            os.pwrite(fd, b"x", 64 * 1024 * 1024 - 1)  # commit a page far outside the region
        finally:
            os.close(fd)

        # Well inside the region size the server still has: only comparing the file against that
        # size can catch this.
        write_varint(stdout, request_id)
        write_varint(stdout, STATUS_OK)
        write_varint(stdout, 0)
        write_varint(stdout, 2)
        stdout.flush()


if __name__ == "__main__":
    main()
