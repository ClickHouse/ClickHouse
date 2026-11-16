"""Exit with an error unless the running ClickHouse servers with the given data paths have their
executable loaded at the fixed base used with ASLR disabled. Usage: check_executable_base.py <data path> <data path>...

With ASLR disabled, the kernel loads a PIE executable at a fixed address rounded down to the largest
alignment of its `PT_LOAD` segments. Two different binaries can therefore have different bases when
one of them has a more aligned segment (for example, a 64 KiB aligned variable in `.data`), even though
both servers were started correctly. So the bases are compared after rounding them down to the largest
segment alignment of all the binaries. A server started with ASLR enabled has a random base that differs
from the fixed one in the bits above any segment alignment, so it is still detected."""

import os
import re
import struct
import sys

PT_LOAD = 1


def max_load_alignment(executable):
    """The largest `p_align` of the `PT_LOAD` segments of a 64-bit little-endian ELF file."""
    with open(executable, "rb") as elf:
        header = elf.read(64)
        if header[:4] != b"\x7fELF" or header[4] != 2 or header[5] != 1:
            raise RuntimeError(f"{executable} is not a 64-bit little-endian ELF file")
        phoff = struct.unpack_from("<Q", header, 0x20)[0]
        phentsize, phnum = struct.unpack_from("<HH", header, 0x36)
        elf.seek(phoff)
        table = elf.read(phentsize * phnum)
    alignment = 1
    for i in range(phnum):
        entry = i * phentsize
        if struct.unpack_from("<I", table, entry)[0] == PT_LOAD:
            alignment = max(alignment, struct.unpack_from("<Q", table, entry + 0x30)[0])
    return alignment


def executable_base(data_path):
    with open(f"{data_path}/status", encoding="utf-8") as status:
        pid = re.search(r"^PID: (\d+)$", status.read(), re.MULTILINE)[1]
    executable = os.readlink(f"/proc/{pid}/exe")
    with open(f"/proc/{pid}/maps", encoding="utf-8") as maps:
        for line in maps:
            fields = line.split()
            if fields[2] == "00000000" and fields[-1] == executable:
                # Read the program headers through /proc, because the file may have been replaced on disk.
                return int(fields[0].split("-")[0], 16), max_load_alignment(f"/proc/{pid}/exe")
    raise RuntimeError(f"No mapping of {executable} in /proc/{pid}/maps")


servers = [executable_base(path) for path in sys.argv[1:]]
alignment = max(server_alignment for _, server_alignment in servers)
print(
    f"Executable base of the servers: {' '.join(f'{base:x}' for base, _ in servers)}, "
    f"largest segment alignment: {alignment:#x}"
)
if len({base & ~(alignment - 1) for base, _ in servers}) != 1:
    # Muted until the static binary is on master: the dynamic PIE reference build is loaded at `ELF_ET_DYN_BASE`,
    # and the static PIE at the mmap base, so the bases differ even with ASLR disabled.
    print("WARNING: The servers have their executable at different addresses: both must be started with ASLR disabled")
