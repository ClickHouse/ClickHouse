#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the DWARF format requires the DWARF parser, which is not built in fasttest.
# no-msan: the DWARF input format is not built under MSan, so the format is unknown there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Bounds and header validation of the ELF container read by the DWARF input format.
# Every fixture is derived from one minimal valid ELF64 little-endian file with a real
# `.debug_abbrev` and `.debug_info` (one DWARF 4 compile unit with a single DIE), so that
# each malformed case differs from the accepted `valid` control in exactly one field and is
# rejected because of that field, not because the DWARF sections are missing.
# Offsets close to UINT64_MAX make unchecked `offset + size` additions overflow.

WORK_DIR="${CLICKHOUSE_TMP}/malformed_elf_$$"
mkdir -p "$WORK_DIR"

python3 - "$WORK_DIR" <<'PY'
import struct, sys, os
work = sys.argv[1]

SHT_PROGBITS, SHT_STRTAB, SHT_NOBITS = 1, 3, 8

def align(n, a=8):
    return (n + a - 1) // a * a

def build(ident_class=2, ident_data=1, ident_version=1, phoff=64, phnum=1, shentsize=64,
          shstrtab_type=SHT_STRTAB, info_type=SHT_PROGBITS, extra_oob_section=False,
          shoff_override=None, phoff_override=None, abbrev_children=0):
    abbrev = bytes([1, 0x11, abbrev_children, 0x03, 0x08, 0, 0, 0])
    die = bytes([1]) + b'a\x00'
    info = struct.pack('<IHIB', 2 + 4 + 1 + len(die), 4, 0, 8) + die
    shstrtab = b'\x00.shstrtab\x00.debug_abbrev\x00.debug_info\x00.comment\x00'
    name_shstrtab, name_abbrev, name_info, name_comment = 1, 11, 25, 37

    data_off = 64 + 56
    shstrtab_off = data_off
    abbrev_off = shstrtab_off + len(shstrtab)
    info_off = abbrev_off + len(abbrev)
    end = info_off + len(info)
    shoff = align(end)

    def sh(name, stype, off, size):
        return struct.pack('<IIQQQQIIQQ', name, stype, 0, 0, off, size, 0, 0, 1, 0)

    sections = [
        sh(0, 0, 0, 0),
        sh(name_shstrtab, shstrtab_type, shstrtab_off, len(shstrtab)),
        sh(name_abbrev, SHT_PROGBITS, abbrev_off, len(abbrev)),
        sh(name_info, info_type, info_off, len(info)),
    ]
    if extra_oob_section:
        sections.append(sh(name_comment, SHT_PROGBITS, 0xFFFFFFFFFFFFFFF0, 0x20))

    e_phoff = phoff if phoff_override is None else phoff_override
    e_shoff = shoff if shoff_override is None else shoff_override
    ident = b'\x7fELF' + bytes([ident_class, ident_data, ident_version]) + bytes(9)
    header = ident + struct.pack('<HHIQQQIHHHHHH', 2, 0x3e, 1, 0, e_phoff, e_shoff, 0,
                                 64, 56, phnum, shentsize, len(sections), 1)
    phdr = struct.pack('<IIQQQQQQ', 1, 4, 0, 0, 0, 0, 0, 8)
    body = header + phdr + shstrtab + abbrev + info
    body += bytes(shoff - len(body))
    return body + b''.join(sections)

cases = {
    'valid': build(),
    'overflow_shoff': build(shoff_override=0xFFFFFFFFFFFFFFF0),
    'overflow_phoff': build(phoff_override=0xFFFFFFFFFFFFFFF0),
    'no_program_headers': build(phoff=0, phnum=0),
    'elf32_class': build(ident_class=1),
    'big_endian_data': build(ident_data=2),
    'bad_ident_version': build(ident_version=0),
    'bad_shentsize': build(shentsize=40),
    'shstrtab_not_strtab': build(shstrtab_type=SHT_PROGBITS),
    'oob_irrelevant_section': build(extra_oob_section=True),
    'nobits_debug_info': build(info_type=SHT_NOBITS),
    'unclosed_tree': build(abbrev_children=1),
}
for name, data in cases.items():
    open(os.path.join(work, name), 'wb').write(data)
open(os.path.join(work, 'too_small'), 'wb').write(b'\x7fELF')
PY

for name in valid overflow_shoff overflow_phoff too_small no_program_headers elf32_class big_endian_data \
    bad_ident_version bad_shentsize shstrtab_not_strtab oob_irrelevant_section nobits_debug_info unclosed_tree
do
    if result=$(${CLICKHOUSE_LOCAL} -q "SELECT count() FROM file('${WORK_DIR}/${name}', DWARF)" 2>"${WORK_DIR}/err")
    then
        echo "${name}: ${result} rows"
    elif grep -q "CANNOT_PARSE_ELF" "${WORK_DIR}/err"
    then
        echo "${name}: rejected ELF"
    elif grep -q "CANNOT_PARSE_DWARF" "${WORK_DIR}/err"
    then
        echo "${name}: rejected DWARF"
    else
        echo "${name}: UNEXPECTED ERROR"
        cat "${WORK_DIR}/err"
    fi
done

rm -rf "$WORK_DIR"
