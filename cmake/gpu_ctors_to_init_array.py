#!/usr/bin/env python3
"""Moves the static constructors of an archive from `.ctors` to `.init_array`, which lld does not do (see cmake/cuda.cmake).

The entries are reversed, as `.ctors` runs backwards, by rewriting relocation offsets; then `llvm-objcopy` renames the sections.

usage: gpu_ctors_to_init_array.py <llvm-objcopy> <input.a> <output.a>
"""

import os
import struct
import subprocess
import sys

SHT_RELA = 4
SHT_REL = 9
SHT_INIT_ARRAY = 14


def reverse_ctors_in_object(data, base):
    """Reverses `.ctors` relocations of the ELF64 LE object at `base` and makes them `SHT_INIT_ARRAY`, in place."""
    if data[base:base + 4] != b"\x7fELF" or data[base + 4] != 2 or data[base + 5] != 1:
        return 0
    e_shoff, = struct.unpack_from("<Q", data, base + 0x28)
    e_shentsize, e_shnum, e_shstrndx = struct.unpack_from("<HHH", data, base + 0x3A)

    def section(i):
        name, kind, _, _, offset, size, _, info, _, entsize = struct.unpack_from("<IIQQQQIIQQ", data, base + e_shoff + i * e_shentsize)
        return name, kind, offset, size, info, entsize

    strtab_offset = section(e_shstrndx)[2]

    def name_of(i):
        start = base + strtab_offset + section(i)[0]
        return bytes(data[start:data.index(b"\0", start)])

    reversed_sections = 0
    for i in range(e_shnum):
        _, kind, offset, size, info, entsize = section(i)
        if kind not in (SHT_RELA, SHT_REL) or name_of(info) != b".ctors":
            continue
        ctors_size = section(info)[3]
        if ctors_size % 8:
            raise RuntimeError("a .ctors section of %d bytes" % ctors_size)
        for entry in range(base + offset, base + offset + size, entsize):
            r_offset, = struct.unpack_from("<Q", data, entry)
            struct.pack_into("<Q", data, entry, ctors_size - 8 - r_offset)
        struct.pack_into("<I", data, base + e_shoff + info * e_shentsize + 4, SHT_INIT_ARRAY)
        reversed_sections += 1
    return reversed_sections


def main():
    objcopy, source, destination = sys.argv[1:4]
    with open(source, "rb") as file:
        data = bytearray(file.read())
    if data[:8] != b"!<arch>\n":
        raise RuntimeError(source + " is not an archive")

    position = 8
    reversed_sections = 0
    while position + 60 <= len(data):
        size = int(data[position + 48:position + 58].decode().strip())
        reversed_sections += reverse_ctors_in_object(data, position + 60)
        position += 60 + size + (size % 2)

    temporary = destination + ".tmp"
    with open(temporary, "wb") as file:
        file.write(data)
    subprocess.run([objcopy, "--rename-section", ".ctors=.init_array", temporary, destination], check=True)
    os.remove(temporary)
    print("%s: moved %d .ctors sections to .init_array" % (os.path.basename(source), reversed_sections))


if __name__ == "__main__":
    main()
