#!/usr/bin/python3

# A pooled worker that answers correctly, stays quiet for longer than the server spends draining its
# `stderr` at hand-back, and only then writes far more than a pipe can hold - before going back to
# read the next request.
#
# The gap is the point. A drain at the moment the worker is handed back sees an empty pipe and can
# say nothing about what the command is going to write next; if that were the only thing standing
# between a chatty command and a blocked worker, this shape would defeat it. What actually keeps the
# promise of `stderr_reaction` `none` is that the read loop of the *next* borrow polls `stderr`
# alongside `stdout` and keeps taking bytes off it while it waits for the response, so a command
# blocked in `write` is unblocked by the very query that is waiting for it.

import mmap
import os
import sys
import time

PROTOCOL_VERSION = 1
STATUS_OK = 0

# Twice the 64 KiB a Linux pipe holds by default, so the command is provably blocked partway.
CHATTER = b"e" * (128 * 1024)

# Comfortably longer than the drain the server performs when it takes the worker back.
QUIET_GAP_SECONDS = 0.3


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

    while True:
        version = read_varint(stdin)
        if version is None:
            break  # stdin closed -> exit
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
                    output += str(os.getpid()).encode("ascii") + b"\n"

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

        # Quiet long enough for the hand-back drain to find nothing, then far too much to fit.
        time.sleep(QUIET_GAP_SECONDS)
        stderr.write(CHATTER)
        stderr.flush()


if __name__ == "__main__":
    main()
