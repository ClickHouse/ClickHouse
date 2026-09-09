#!/usr/bin/python3

# A UDF that never answers, and keeps talking on its `stderr` while not answering.
#
# `command_read_timeout` says how long the server may wait for the command to say something on its
# `stdout`. Both descriptors are polled - `stderr_reaction` `none` still has to take those bytes off
# the pipe, or the command blocks in `write` - so every one of these chunks wakes that wait up. A
# wait that restarts its budget at each wake-up is no longer bounded by anything the command does
# not control: this one produces nothing the query can ever use, and would hold it open for as long
# as it cared to keep writing. The timeout has to be one budget for the whole read.
#
# The pause between chunks is deliberate: it is what makes the wake-ups a stream of separate events
# rather than one long readable stretch, which is the shape that resets a per-wake-up budget.

import sys
import time


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


def main():
    stdin = sys.stdin.buffer
    stderr = sys.stderr.buffer

    # Read the request in full, then never write a byte to stdout.
    read_varint(stdin)
    path_length = read_varint(stdin)
    if path_length is not None:
        stdin.read(path_length)
        read_varint(stdin)
        read_varint(stdin)

    while True:
        # Far more often than the function's `command_read_timeout`, so a budget that restarts on
        # each of these never runs out.
        stderr.write(b"still here\n")
        stderr.flush()
        time.sleep(0.05)


if __name__ == "__main__":
    main()
