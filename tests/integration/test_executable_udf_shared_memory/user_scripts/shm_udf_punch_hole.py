#!/usr/bin/python3

# A pooled shared-memory UDF that answers correctly and frees every page of its region's file past
# its answer with `fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)` - which the seals do not
# stop: they forbid making the file shorter, and the file stays exactly as long. Otherwise
# identical to shm_udf.py. What the server does about it is the test's business.
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

import ctypes
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
    output = bytearray()
    for line in input_data.split(b"\n"):
        if line == b"":
            continue
        output += b"Key " + line + b"\n"

    output_offset = len(input_data)  # write the result right after the input
    if output_offset + len(output) > region_size:
        raise NeedMoreSpace(output_offset + len(output))

    region[output_offset : output_offset + len(output)] = bytes(output)
    region.flush()
    return output_offset, len(output)


FALLOC_FL_KEEP_SIZE = 0x01
FALLOC_FL_PUNCH_HOLE = 0x02


def punch_hole(fd, offset):
    # Frees every page from `offset` (rounded up to a page) to the end of the file. The file keeps
    # its length; the pages are simply gone until somebody writes to them again.
    page = mmap.PAGESIZE
    offset = (offset + page - 1) // page * page
    size = os.fstat(fd).st_size
    if offset >= size:
        return
    libc = ctypes.CDLL(None, use_errno=True)
    libc.fallocate.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_int64, ctypes.c_int64]
    if libc.fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, size - offset) != 0:
        raise OSError(ctypes.get_errno(), "fallocate(FALLOC_FL_PUNCH_HOLE) failed")


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

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
                try:
                    region_size = len(region)
                    input_data = region[input_offset : input_offset + input_size]
                    output_offset, output_size = process(input_data, region, region_size)
                finally:
                    region.close()
            finally:
                os.close(fd)

            # Before the response goes out, so that the test finds the hole there the moment the
            # query returns: everything past this request's input and output is freed. Not the
            # answer itself - the server is about to read it, and a command that wiped its own
            # answer would just fail its own query.
            punch_fd = os.open(path, os.O_RDWR)
            try:
                punch_hole(punch_fd, output_offset + output_size)
            finally:
                os.close(punch_fd)

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
