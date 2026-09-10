#!/usr/bin/python3

# A pooled UDF that writes far more diagnostics to its `stderr` than a pipe can hold, before it
# answers. Configured with `stderr_reaction` `none`.
#
# "None" says what to do with those bytes - nothing - not that the pipe may be left unread. A server
# that stops polling `stderr` because it has no use for it lets the pipe fill up, and the command
# then blocks in `write` with its answer unwritten: the request times out, and for a pooled process
# the leftovers carry over into whichever query borrows it next. So the bytes have to be read and
# discarded, and this command has to be answered normally, every time, on the same worker.
#
# It answers with its own pid, so a test can tell a reused worker from a fresh one.

import mmap
import os
import sys

PROTOCOL_VERSION = 1
STATUS_OK = 0

# Comfortably past the 64 KiB a Linux pipe holds by default, so the write blocks unless it is read.
CHATTER_SIZE = 4 * 1024 * 1024


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
        request_id = read_varint(stdin)
        if version != PROTOCOL_VERSION:
            raise RuntimeError(f"unsupported protocol version {version}")

        path_length = read_varint(stdin)
        path = stdin.read(path_length).decode("utf-8")
        input_offset = read_varint(stdin)
        input_size = read_varint(stdin)

        # Before the answer, which is where diagnostics belong - and where they deadlock a server
        # that is waiting for a response it will never get.
        stderr.write(b"d" * CHATTER_SIZE)
        stderr.flush()

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


if __name__ == "__main__":
    main()
