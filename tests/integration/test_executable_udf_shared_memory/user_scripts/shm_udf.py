#!/usr/bin/python3

# Executable UDF that exchanges data through a shared-memory file instead of the pipes.
#
# Protocol (all control values use the ClickHouse native binary encoding):
#   server -> stdin : varint version, varint request id, varint path length + path bytes,
#                     varint input offset, varint input size
#   stdout <- server: the request id echoed back, varint status (0 = ok), then on success
#                     varint output offset + varint output size; status 2 asks the server for a
#                     larger region and is followed by the varint total size needed; any other
#                     status is followed by a length-prefixed error message
# The bulk data lives in the shared-memory file at the given path; the pipes carry only
# these small control commands. When stdin reaches EOF the process exits.

import mmap
import os
import sys

PROTOCOL_VERSION = 1
STATUS_OK = 0
STATUS_ERROR = 1
STATUS_NEED_MORE_SPACE = 2


class NeedMoreSpace(Exception):
    def __init__(self, required_size):
        super().__init__(
            f"the shared-memory region must be at least {required_size} bytes"
        )
        self.required_size = required_size


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


def write_string_binary(stream, text):
    encoded = text.encode("utf-8")
    write_varint(stream, len(encoded))
    stream.write(encoded)


def process(input_data, region, region_size):
    # A function without arguments: the request carries no input at all (offset 0, size 0) and the
    # answer is one row that depends on none. The server still has to make that request - there is
    # nothing else that would make this command produce a row.
    if "--zero-argument" in sys.argv:
        output = b"42\n"
        output_offset = len(input_data)
        if output_offset + len(output) > region_size:
            raise NeedMoreSpace(output_offset + len(output))
        region[output_offset : output_offset + len(output)] = output
        region.flush()
        return output_offset, len(output)

    # Input format is TabSeparated: one UInt64 per line.
    output = bytearray()
    for line in input_data.split(b"\n"):
        if line == b"":
            continue
        if "--report-pid" in sys.argv:
            output += str(os.getpid()).encode("ascii") + b"\n"
        else:
            output += b"Key " + line + b"\n"

    output_offset = len(input_data)  # write the result right after the input
    if output_offset + len(output) > region_size:
        # Only the server can resize the region: ask it for one that fits and it re-sends the
        # same request over the larger mapping.
        raise NeedMoreSpace(output_offset + len(output))

    region[output_offset : output_offset + len(output)] = bytes(output)
    region.flush()
    return output_offset, len(output)


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    # A line on stderr before the first request is ever read: what a command that logs its startup
    # writes. It belongs to the query that started the process, and under `stderr_reaction`
    # `throw` it fails that query - the process is new, so there is no earlier invocation for the
    # server to pin it on.
    if "--stderr-at-startup" in sys.argv:
        sys.stderr.write("starting up\n")
        sys.stderr.flush()

    while True:
        version = read_varint(stdin)
        if version is None:
            break  # stdin closed -> exit
        request_id = read_varint(stdin)

        path_length = read_varint(stdin)
        path = stdin.read(path_length).decode("utf-8")
        input_offset = read_varint(stdin)
        input_size = read_varint(stdin)

        try:
            if version != PROTOCOL_VERSION:
                raise ValueError(f"unsupported protocol version {version}")

            fd = os.open(path, os.O_RDWR)
            try:
                region = mmap.mmap(fd, 0)
            finally:
                os.close(fd)

            try:
                region_size = len(region)
                input_data = region[input_offset : input_offset + input_size]
                output_offset, output_size = process(input_data, region, region_size)
            finally:
                region.close()

            write_varint(stdout, request_id)
            write_varint(stdout, STATUS_OK)
            write_varint(stdout, output_offset)
            write_varint(stdout, output_size)
        except NeedMoreSpace as need_more_space:
            write_varint(stdout, request_id)
            write_varint(stdout, STATUS_NEED_MORE_SPACE)
            write_varint(stdout, need_more_space.required_size)
        except Exception as exception:  # noqa: BLE001
            write_varint(stdout, request_id)
            write_varint(stdout, STATUS_ERROR)
            write_string_binary(stdout, str(exception))

        stdout.flush()


if __name__ == "__main__":
    main()
