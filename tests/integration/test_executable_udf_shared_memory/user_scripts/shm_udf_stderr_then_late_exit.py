#!/usr/bin/python3
# A pooled shared-memory UDF that answers, waits out the hand-back probe, writes a diagnostic to
# stderr and exits in the pool. See input_pool_stderr_then_late_exit.py for the pipe counterpart.
import mmap
import os
import sys
import time

PROTOCOL_VERSION = 1
STATUS_OK = 0

QUIET_GAP_SECONDS = float(sys.argv[sys.argv.index("--gap") + 1]) if "--gap" in sys.argv else 0.3


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
                # The pid, so that the test can tell a replacement from a reused worker.
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

    # Wait out the hand-back probe of the query just served, then complain and go: the worker dies
    # in the pool with its last words unread, and the next borrow has to report them.
    time.sleep(QUIET_GAP_SECONDS)
    stderr.write(b"last words of the worker\n")
    stderr.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
