#!/usr/bin/python3

# A pooled shared-memory UDF that answers correctly and then commits pages past the end of its
# region's file with `fallocate(FALLOC_FL_KEEP_SIZE)` - twice the file's length of them. The seals
# do not stop that (only shrinking is sealed), and the file's length does not change, so a server
# that measured its regions by their length alone would never see those pages. Otherwise identical
# to shm_udf.py. What the server does about it is the test's business.
#
# With an argument, the pages go somewhere else: three of them (or as many as fit in the number
# of bytes the second argument gives - bytes, so that a configuration means the same on every
# page size) at that absolute offset, far past the end of the file, the same ones on every call.
# A growth of the file that stops short of them commits its own pages on top of them, not instead
# of them. A third argument stretches the file to that length first, without committing a page
# (`ftruncate`): length without pages, next to pages without length. The pages are committed
# before the request is answered, whatever the answer - including a request for a larger region.
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


def allocate_beyond_eof(fd, far_offset, far_bytes, sparse_length):
    # Commits twice the file's length of pages past its end - at least three pages, for a file
    # shorter than a page - without moving the end: `st_size` stays what it was, `st_blocks` grows.
    # Or, given `far_offset`, the whole pages of `far_bytes` there (three pages by default): the
    # same ones every time, so that repeating this commits nothing more - after stretching the
    # file to `sparse_length`, if given.
    if sparse_length is not None and os.fstat(fd).st_size < sparse_length:
        os.ftruncate(fd, sparse_length)
    if far_offset is None:
        size = os.fstat(fd).st_size
        offset, length = size, max(2 * size, 3 * mmap.PAGESIZE)
    else:
        offset = far_offset
        length = 3 * mmap.PAGESIZE if far_bytes is None else far_bytes // mmap.PAGESIZE * mmap.PAGESIZE
    libc = ctypes.CDLL(None, use_errno=True)
    libc.fallocate.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_int64, ctypes.c_int64]
    if libc.fallocate(fd, FALLOC_FL_KEEP_SIZE, offset, length) != 0:
        raise OSError(ctypes.get_errno(), "fallocate(FALLOC_FL_KEEP_SIZE) failed")


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
    far_offset = int(sys.argv[1]) if len(sys.argv) > 1 else None
    far_bytes = int(sys.argv[2]) if len(sys.argv) > 2 else None
    sparse_length = int(sys.argv[3]) if len(sys.argv) > 3 else None

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

            # Before any response goes out - the answer, or a request for a larger region - so
            # that the pages are there by the time the server acts on it.
            alloc_fd = os.open(path, os.O_RDWR)
            try:
                allocate_beyond_eof(alloc_fd, far_offset, far_bytes, sparse_length)
            finally:
                os.close(alloc_fd)

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
