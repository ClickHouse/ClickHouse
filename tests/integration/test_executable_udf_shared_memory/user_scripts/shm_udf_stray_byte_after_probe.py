#!/usr/bin/python3

# A pooled worker that answers correctly and then, well after the server has stopped looking, writes
# one stray byte to its `stdout`.
#
# The pause is what makes it interesting. The server probes the worker's pipes when it takes it back
# and refuses to pool one that left anything behind - but a probe is one instant, and this byte
# arrives after it. The worker goes back into the pool looking clean, and the byte is waiting there
# for whoever borrows it next.
#
# What that byte costs depends entirely on whether the protocol can tell one answer from another. As
# a bare status varint it is a plausible frame: `0` reads as success, the real status becomes the
# offset, the real offset becomes the size - and with a compatible format the next query gets the
# region's own *input* back as its result, in the right number of rows, with no error anywhere. The
# request id is what makes that impossible: the next borrow reads this byte where its own id should
# be, sees a mismatch, and fails loudly instead of answering wrongly.

import mmap
import os
import sys
import threading
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

        # Long after the server has taken this worker back and pronounced it clean.
        def litter():
            time.sleep(1)
            stdout.write(b"\x00")
            stdout.flush()

        threading.Thread(target=litter, daemon=True).start()


if __name__ == "__main__":
    main()
