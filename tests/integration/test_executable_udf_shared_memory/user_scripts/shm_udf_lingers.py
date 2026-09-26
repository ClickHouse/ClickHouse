#!/usr/bin/python3

# A UDF that answers correctly and then refuses to leave: on `stdin` EOF - the server telling it to
# exit - it sleeps far past `command_termination_timeout` and only then exits non-zero.
#
# `check_exit_code` says the command's exit status is checked and a non-zero one fails the query.
# The server cannot read a status from a process that has not exited, and it will not wait for one
# indefinitely either - `command_termination_timeout` is what it waits, after which the process is
# signalled. A status that could not be read is not a passing status: waving the query through with
# a log line would make `check_exit_code` mean "checked, unless the command avoids being checked",
# which is precisely the command it most needs to hold for. This one exits `1` in the end, and
# nothing would ever see it.

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

    while True:
        version = read_varint(stdin)
        if version is None:
            break  # stdin closed -> the server is waiting for this process to exit
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

    # Far past the function's `command_termination_timeout`. The SIGTERM that follows ends this
    # sleep, so the process does not outlive the query by more than the signal takes to arrive.
    time.sleep(600)
    sys.exit(1)


if __name__ == "__main__":
    main()
