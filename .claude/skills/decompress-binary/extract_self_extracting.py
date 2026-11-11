#!/usr/bin/env python3
"""Extract the inner ELF from a ClickHouse self-extracting executable.

ClickHouse release/CI `clickhouse` binaries are self-extracting: a small
decompressor stub followed by the zstd-compressed real ELF and a trailer.
Running the binary self-extracts by re-`exec`-ing itself, which only works on
the binary's own architecture. To inspect a foreign-arch binary on your host
(e.g. load an aarch64 CI core dump on an x86 workstation) you must extract the
payload offline, which is what this script does.

Layout (see utils/self-extracting-executable/types.h):

    [ decompressor ELF ]
    [ compressed file blobs ]
    [ FileData[] ]                      # one per packed file, each followed by its name
    [ MetaData (16 bytes) @ EOF ]

    MetaData { uint64 number_of_files; uint64 start_of_files_data; }
    FileData { uint64 start, end, name_length, uncompressed_size, umask; bool exec; }
              # 41 payload bytes, padded to 48; name (name_length bytes) follows each FileData
    Compressed blob for a file is input[start:end] (zstd, possibly multi-frame).

Usage:
    extract_self_extracting.py <clickhouse-self-extracting> [output.elf]
"""
import struct
import subprocess
import sys

FILEDATA_FMT = "<QQQQQ?"  # start, end, name_length, uncompressed_size, umask, exec
FILEDATA_SIZE = 48        # struct is padded to 48 bytes in C++


def main() -> int:
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    path = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else "clickhouse.elf"

    data = open(path, "rb").read()
    number_of_files, start_of_files = struct.unpack_from("<QQ", data, len(data) - 16)
    print(f"number_of_files={number_of_files} start_of_files_data={start_of_files}")
    if not 0 < number_of_files < 1000:
        sys.exit("implausible file count; not a self-extracting binary or it is truncated")

    extracted = False
    pos = start_of_files
    for i in range(number_of_files):
        start, end, name_len, usize, _umask, is_exec = struct.unpack_from(FILEDATA_FMT, data, pos)
        name = data[pos + FILEDATA_SIZE : pos + FILEDATA_SIZE + name_len].decode("utf-8", "replace")
        print(f"  file[{i}] name={name!r} exec={is_exec} comp=[{start},{end}) ({end - start} B) uncompressed={usize}")
        if is_exec:
            blob = data[start:end]
            res = subprocess.run(["zstd", "-d", "-f", "-o", out], input=blob,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if res.returncode != 0:
                sys.exit("zstd failed: " + res.stderr.decode()[:500])
            print(f"  -> extracted inner ELF to {out}")
            extracted = True
        pos += FILEDATA_SIZE + name_len

    if not extracted:
        sys.exit("no executable file found inside the archive")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
