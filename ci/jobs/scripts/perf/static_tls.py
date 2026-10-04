#!/usr/bin/env python3
"""Give the two compared ClickHouse servers the same static TLS size.

glibc allocates the static TLS block at the top of every thread's stack, so the
executable's PT_TLS size decides where each thread's stack starts. Extending the
static TLS surplus of the smaller binary puts the threads of both servers at the
same stack placement.

Usage: static_tls.py <left binary> <right binary>. Prints one line per server,
"env GLIBC_TUNABLES=..." or empty; exits non-zero if the binaries cannot be
compared.
"""

import os
import struct
import sys

PT_TLS = 7
EM_X86_64 = 62
EM_AARCH64 = 183
TUNABLE = "glibc.rtld.optional_static_tls"
DEFAULT_OPTIONAL_STATIC_TLS = 512


def read_static_tls(path):
    """Return (e_machine, p_memsz, p_align) of the PT_TLS segment of an ELF64 LE file."""
    with open(path, "rb") as f:
        header = f.read(64)
        if len(header) < 64 or header[:6] != b"\x7fELF\x02\x01":
            raise ValueError(f"{path} is not a little-endian ELF64 file")
        machine = struct.unpack_from("<H", header, 0x12)[0]
        phoff = struct.unpack_from("<Q", header, 0x20)[0]
        phentsize, phnum = struct.unpack_from("<HH", header, 0x36)
        f.seek(phoff)
        phdrs = f.read(phentsize * phnum)
    if phentsize < 56 or len(phdrs) != phentsize * phnum:
        raise ValueError(f"{path} has a malformed program header table")
    for offset in range(0, len(phdrs), phentsize):
        if struct.unpack_from("<I", phdrs, offset)[0] == PT_TLS:
            memsz, align = struct.unpack_from("<QQ", phdrs, offset + 0x28)
            if memsz < 4096:
                raise ValueError(
                    f"{path} has {memsz} bytes of static TLS, too few for a "
                    "ClickHouse server (a self-extracting binary not decompressed yet?)"
                )
            return machine, memsz, align
    raise ValueError(f"{path} has no PT_TLS segment")


def static_tls_tunables(left, right):
    """Return the GLIBC_TUNABLES values for the (left, right) servers, "" where none is needed."""
    machine, left_size, align = read_static_tls(left)
    right_machine, right_size, right_align = read_static_tls(right)
    if (machine, align) != (right_machine, right_align):
        raise ValueError(
            f"machine/TLS alignment differ: {machine}/{align} in {left}, "
            f"{right_machine}/{right_align} in {right}"
        )
    # glibc 2.39 layout, re-validate on a glibc upgrade: x86_64 rounds the
    # executable's block to its alignment, aarch64 aligns the next block (libc's) to 16.
    granule = {EM_X86_64: align, EM_AARCH64: 16}.get(machine)
    if not granule:
        raise ValueError(
            f"unsupported machine {machine}, TLS alignment {align} in {left}"
        )
    existing = os.environ.get("GLIBC_TUNABLES", "")
    if TUNABLE in existing:
        raise ValueError(f"GLIBC_TUNABLES already sets {TUNABLE}: {existing}")

    def rounded(size):
        return (size + granule - 1) // granule * granule

    pad = rounded(right_size) - rounded(left_size)
    value = ":".join(
        filter(None, [existing, f"{TUNABLE}={DEFAULT_OPTIONAL_STATIC_TLS + abs(pad)}"])
    )
    padded = "left" if pad > 0 else "right"
    note = (
        f"{abs(pad)} bytes of surplus on {padded}" if pad else "same layout, no padding"
    )
    print(
        f"static TLS: left {left_size}, right {right_size} bytes; {note}",
        file=sys.stderr,
    )
    return (value if pad > 0 else "", value if pad < 0 else "")


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <left binary> <right binary>", file=sys.stderr)
        return 2
    try:
        tunables = static_tls_tunables(sys.argv[1], sys.argv[2])
    except (OSError, ValueError) as e:
        print(f"static TLS: cannot compare the servers: {e}", file=sys.stderr)
        return 1
    for value in tunables:
        print(f"env GLIBC_TUNABLES={value}" if value else "")
    return 0


if __name__ == "__main__":
    sys.exit(main())
