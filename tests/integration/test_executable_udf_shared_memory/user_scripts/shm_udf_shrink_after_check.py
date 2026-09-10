#!/usr/bin/python3

# Misbehaving UDF that truncates its region in the one instant the checks cannot cover: after the
# server has verified the file is still whole, and before it uses the bytes of the response.
#
# The server checks the region immediately after reading `status`, so the truncation is delayed
# until after that byte has been sent and answered for. The response that follows is a perfectly
# valid one, pointing at an offset well inside the size the server still believes the region has -
# so the bounds check passes too, and the server goes on to read output that the file no longer
# holds. Read through the mapping that is a `SIGBUS`, which no handler can turn back into a failed
# query: it takes the whole server down, and every unrelated query on it. Read with `pread` it is a
# short read, and only this query dies.

import os
import sys
import time

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

        # The status alone. The server checks the region as soon as it has this, and finds it whole.
        write_varint(stdout, request_id)
        write_varint(stdout, STATUS_OK)
        stdout.flush()
        time.sleep(1)

        # Now that the check is behind us, take the pages away.
        fd = os.open(path, os.O_RDWR)
        try:
            os.ftruncate(fd, 0)
        finally:
            os.close(fd)

        # ... and finish a response the bounds check has no reason to reject.
        write_varint(stdout, 0)
        write_varint(stdout, 2)
        stdout.flush()


if __name__ == "__main__":
    main()
