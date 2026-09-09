#!/usr/bin/python3

# A pooled UDF that burns a measurable amount of CPU answering, and then writes one byte past its
# response frame - so the server discards the worker instead of returning it to the pool.
#
# That discard is what this is for. The borrow's CPU and peak resident set are read out of
# `/proc/<pid>`, so they have to be read while the pid is still there: closing the child's stdin
# makes it exit, and a zombie has no `VmHWM` left, while the wait that follows reaps the pid
# altogether. A server that samples after any of that reports zero for exactly the borrows whose
# accounting matters most - the ones that went wrong.
#
# The stray byte goes out in the same write as the response frame, so the worker is provably dirty
# by the time the server looks, rather than depending on which process is scheduled first.

import mmap
import os
import sys

PROTOCOL_VERSION = 1
STATUS_OK = 0

# Enough to be several times the 10 ms tick that `/proc/<pid>/stat` counts CPU in.
CPU_ITERATIONS = 400000


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


def encode_varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            break
    return bytes(out)


def burn_cpu():
    acc = 0
    for i in range(CPU_ITERATIONS):
        acc = (acc + i * i) % 1000003
    return acc


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    while True:
        version = read_varint(stdin)
        if version is None:
            break  # stdin closed -> exit
        if version != PROTOCOL_VERSION:
            raise RuntimeError(f"unsupported protocol version {version}")

        path_length = read_varint(stdin)
        path = stdin.read(path_length).decode("utf-8")
        input_offset = read_varint(stdin)
        input_size = read_varint(stdin)

        burn_cpu()

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

        # Response frame plus one byte that is part of nothing, in a single write.
        stdout.write(
            encode_varint(STATUS_OK)
            + encode_varint(output_offset)
            + encode_varint(len(output))
            + b"\n"
        )
        stdout.flush()


if __name__ == "__main__":
    main()
