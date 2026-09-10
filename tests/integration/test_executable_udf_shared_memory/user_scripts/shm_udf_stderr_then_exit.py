#!/usr/bin/python3

# Answers correctly, writes a long diagnostic to `stderr` and exits in the same breath - no pause
# anywhere.
#
# This is the timing the other late-stderr scripts deliberately avoid, and the one that catches a
# server which reaps before it reads: reaping closes the child's pipes, and everything the child had
# written and nobody had read yet goes with them. Under `stderr_reaction` `throw` the query would
# then succeed while the command was shouting.
#
# The size is chosen deliberately: comfortably more than one 4 KiB read, so picking it up takes
# several, and comfortably less than a pipeful, so the command is never blocked in `write` and can
# really exit in the same breath. A message larger than the pipe would block the writer, keep the
# process alive, and quietly turn this into a test of something else.

import mmap
import os
import sys

PROTOCOL_VERSION = 1
STATUS_OK = 0

COMPLAINT = b"e" * 8192


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
    stderr = sys.stderr.buffer

    version = read_varint(stdin)
    if version != PROTOCOL_VERSION:
        raise RuntimeError(f"unsupported protocol version {version}")
    request_id = read_varint(stdin)

    path_length = read_varint(stdin)
    path = stdin.read(path_length).decode("utf-8")
    input_offset = read_varint(stdin)
    input_size = read_varint(stdin)

    fd = os.open(path, os.O_RDWR)
    try:
        region = mmap.mmap(fd, 0)
    finally:
        os.close(fd)

    try:
        output = bytearray()
        for line in region[input_offset : input_offset + input_size].split(b"\n"):
            if line != b"":
                output += b"Key " + line + b"\n"

        output_offset = input_size
        region[output_offset : output_offset + len(output)] = bytes(output)
        region.flush()
    finally:
        region.close()

    write_varint(stdout, request_id)
    write_varint(stdout, STATUS_OK)
    write_varint(stdout, output_offset)
    write_varint(stdout, len(output))
    stdout.flush()

    # No pause: complain and go. A pipeful of this is still in flight when the process is gone.
    stderr.write(COMPLAINT)
    stderr.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
