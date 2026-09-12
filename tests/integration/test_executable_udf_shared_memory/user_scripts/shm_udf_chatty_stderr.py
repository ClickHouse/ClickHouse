#!/usr/bin/python3

# Misbehaving pooled UDF: it answers every request correctly and then, a moment later, writes a line
# to its `stderr`. Nothing in this invocation notices - stderr is read together with the response,
# and by the time this line is written that read is over. The next request on the same process is
# the one that drains it and reports it as *its* output; under `stderr_reaction` `throw` that next
# query fails, for something a previous query's arguments caused. The server must therefore refuse
# to return this worker to the pool.
#
# The pause before the write is what makes the misbehaviour reproducible rather than accidental. A
# line written immediately after the response does not leak at all: the server is asleep in `poll`
# waiting for that response, and a woken `poll` re-scans the descriptors it was given, so a line
# written within the thread's wake-up latency - microseconds - is reported ready along with the
# response and drained into the query that earned it. Only a command that writes later, once that
# read is over, leaves anything behind. This one waits long enough to be sure it is that command,
# and briefly enough to still be inside the borrow (the server is parsing the answer, which the test
# makes take far longer than this).
#
# It answers with its own pid, so a test can tell a fresh worker from a reused one.

import mmap
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

        # Too late: the response above is what the server was waiting for, and it has long stopped
        # reading by now, so this belongs to nobody until the next borrow picks it up.
        time.sleep(0.002)
        stderr.write(b"done\n")
        stderr.flush()


if __name__ == "__main__":
    main()
