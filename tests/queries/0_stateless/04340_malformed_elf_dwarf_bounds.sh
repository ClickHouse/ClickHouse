#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the DWARF format requires the DWARF parser, which is not built in fasttest.
# no-msan: the DWARF input format is not built under MSan (LLVM, which the DWARF parser depends on, is excluded from MSan builds), so the format is unknown there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for bounds checking in Common/Elf (used by the DWARF input format).
# Feed deliberately malformed ELF files to the DWARF format and check that each is rejected
# with a clean exception instead of reading out of bounds. The crafted offsets are close to
# UINT64_MAX so that the unchecked `offset + size` additions would overflow and pass naive
# bounds checks, leading to wild pointer dereferences. All these inputs are rejected during
# ELF header validation, before any DWARF parsing, so the test is sanitizer-safe.

WORK_DIR="${CLICKHOUSE_TMP}/malformed_elf_$$"
mkdir -p "$WORK_DIR"

python3 - "$WORK_DIR" <<'PY'
import struct, sys, os
work = sys.argv[1]

def elf_header(shoff, shnum, phoff, phnum, shstrndx=0):
    # 64-byte ELF64 header.
    return (b'\x7fELF' + bytes(12)
        + struct.pack('<HHIQQQIHHHHHH', 2, 0x3e, 1, 0, phoff, shoff, 0, 64, 56, phnum, 64, shnum, shstrndx))

def section_header(name, stype, offset, size):
    # 64-byte ELF64 section header.
    return struct.pack('<IIQQQQIIQQ', name, stype, 0, 0, offset, size, 0, 0, 0, 0)

# Section header table offset close to UINT64_MAX: shoff + shnum * entsize overflows.
open(os.path.join(work, 'overflow_shoff'), 'wb').write(elf_header(0xFFFFFFFFFFFFFFF0, 1, 64, 1))

# Valid section header and section-names string table, but program header table offset close to
# UINT64_MAX so that phoff + phnum * entsize overflows. Reaches the program header check.
SHT_STRTAB = 3
body = elf_header(shoff=64, shnum=1, phoff=0xFFFFFFFFFFFFFFF0, phnum=1, shstrndx=0)
body += section_header(name=0, stype=SHT_STRTAB, offset=128, size=1)
body += b'\x00'
open(os.path.join(work, 'overflow_phoff'), 'wb').write(body)

# Truncated: only the ELF magic.
open(os.path.join(work, 'too_small'), 'wb').write(b'\x7fELF')

# Valid magic, everything else zero (no section headers).
open(os.path.join(work, 'no_sections'), 'wb').write(elf_header(0, 0, 0, 0) + bytes(64))
PY

for name in overflow_shoff overflow_phoff too_small no_sections
do
    if ${CLICKHOUSE_LOCAL} -q "SELECT count() FROM file('${WORK_DIR}/${name}', DWARF)" >/dev/null 2>"${WORK_DIR}/err"
    then
        echo "${name}: UNEXPECTED SUCCESS"
    elif grep -q "CANNOT_PARSE_ELF" "${WORK_DIR}/err"
    then
        echo "${name}: rejected"
    else
        echo "${name}: UNEXPECTED ERROR"
        cat "${WORK_DIR}/err"
    fi
done

rm -rf "$WORK_DIR"
