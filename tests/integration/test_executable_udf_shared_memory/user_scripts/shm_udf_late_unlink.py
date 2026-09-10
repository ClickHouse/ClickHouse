#!/usr/bin/python3

# Misbehaving pooled UDF: on its first request it lets the server complete the response-side
# integrity check, then unlinks the region before finishing a valid response. The final cleanup
# check must discard this worker; a reused worker would fail to open the missing path next time.

import os
import sys
import time

PROTOCOL_VERSION = 1
STATUS_OK = 0
MARKER = "/tmp/clickhouse_shm_udf_late_unlink_once"


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
        request_id = read_varint(stdin)
        if version != PROTOCOL_VERSION:
            raise RuntimeError(f"unsupported protocol version {version}")

        path_length = read_varint(stdin)
        path = stdin.read(path_length).decode("utf-8")
        output_offset = read_varint(stdin)
        output_size = read_varint(stdin)

        # Every request opens the supplied name. If the damaged worker is incorrectly reused, its
        # second request fails here; a replacement process receives a fresh region path.
        fd = os.open(path, os.O_RDWR)
        os.close(fd)

        first_request = not os.path.exists(MARKER)
        write_varint(stdout, request_id)
        write_varint(stdout, STATUS_OK)
        if first_request:
            stdout.flush()
            time.sleep(1)
            os.unlink(path)
            with open(MARKER, "w", encoding="utf-8"):
                pass

        write_varint(stdout, output_offset)
        write_varint(stdout, output_size)
        stdout.flush()


if __name__ == "__main__":
    main()
