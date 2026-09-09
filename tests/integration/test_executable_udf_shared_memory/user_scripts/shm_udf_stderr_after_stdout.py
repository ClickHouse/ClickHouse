#!/usr/bin/python3

# A UDF that answers correctly, closes its `stdout`, and only then writes far more to `stderr` than
# a pipe can hold - the shape a command has when it dumps a summary on its way out.
#
# Configured with `stderr_reaction` `none`. "None" says what to do with those bytes - nothing - not
# that the pipe may be left unread: nobody is reading it any more once `stdout` has ended, so the
# command blocks in `write` and never reaches its own exit. A server that then reaps it with a
# blocking `waitpid` waits for a process that is waiting for the server, and the query hangs with
# its result already computed. Both halves have to hold: the rest of `stderr` is taken off the pipe
# when `stdout` ends, whatever the reaction, and the wait that reaps the command is bounded and
# keeps draining while it waits.

import mmap
import os
import sys

PROTOCOL_VERSION = 1
STATUS_OK = 0

# Comfortably past the 64 KiB a Linux pipe holds by default.
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

    version = read_varint(stdin)
    if version != PROTOCOL_VERSION:
        raise RuntimeError(f"unsupported protocol version {version}")

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

    write_varint(stdout, STATUS_OK)
    write_varint(stdout, output_offset)
    write_varint(stdout, len(output))
    stdout.flush()

    # The answer is complete; end the conversation and only then start talking.
    os.close(1)

    stderr.write(b"e" * CHATTER_SIZE)
    stderr.flush()

    sys.exit(0)


if __name__ == "__main__":
    main()
