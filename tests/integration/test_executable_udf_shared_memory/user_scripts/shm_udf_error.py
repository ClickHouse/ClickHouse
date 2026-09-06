#!/usr/bin/python3

# Misbehaving UDF: reads a request and always answers through the protocol's error channel
# (a non-zero status followed by a length-prefixed message). The server must fail the query
# with that message.

import sys

STATUS_ERROR = 1


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


def write_string_binary(stream, text):
    encoded = text.encode("utf-8")
    write_varint(stream, len(encoded))
    stream.write(encoded)


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    while True:
        version = read_varint(stdin)
        if version is None:
            break

        path_length = read_varint(stdin)
        stdin.read(path_length)
        read_varint(stdin)  # input offset
        read_varint(stdin)  # input size

        write_varint(stdout, STATUS_ERROR)
        write_string_binary(stdout, "the command cannot process this request")
        stdout.flush()


if __name__ == "__main__":
    main()
