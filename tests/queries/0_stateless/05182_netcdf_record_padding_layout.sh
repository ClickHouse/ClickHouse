#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# Writes files with two `byte` record variables. The slab of every record variable is padded to
# four bytes, so the reader takes the record to be eight bytes and reads the record `i` of a
# variable at `begin + i * 8`. A header that packs the two slabs tighter than that, or leaves a gap
# between them, describes a layout the reader cannot follow: only the first record would be read
# from the right place, so such a file has to be rejected instead of serving padding as the data of
# the later records.
write_file()
{
    python3 - "$1" "$2" <<'PYTHON'
import struct
import sys

def tag(value):
    return struct.pack('>i', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

NC_DIMENSION, NC_VARIABLE, NC_BYTE, ABSENT = 10, 11, 1, tag(0) + tag(0)

kind, path = sys.argv[1], sys.argv[2]

# The offset of the slab of `b` inside the record: right after the unpadded slab of `a`, one byte
# past the padded end of the slab of `a`, and at that padded end, which is the layout the format
# prescribes.
offset_b = {'under-padded': 1, 'gap': 5, 'packed': 4}[kind]

header = b'CDF\x01' + tag(2)
header += tag(NC_DIMENSION) + tag(1) + name('t') + tag(0)
header += ABSENT
header += tag(NC_VARIABLE) + tag(2)
header += name('a') + tag(1) + tag(0) + ABSENT + tag(NC_BYTE) + tag(4) + tag(116)
header += name('b') + tag(1) + tag(0) + ABSENT + tag(NC_BYTE) + tag(4) + tag(116 + offset_b)
assert len(header) == 116, len(header)

with open(path, 'wb') as out:
    # Two records of eight bytes, which is what the padded slabs add up to. The values of `a` are
    # 1 and 3, the values of `b` are 2 and 4 when the file is packed the way the format says.
    out.write(header + b'\x01\x00\x00\x00\x02\x00\x00\x00\x03\x00\x00\x00\x04\x00\x00\x00')
PYTHON
}

echo "--- the slab of the second record variable begins before the padded end of the first one"
write_file under-padded "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)" 2>&1 | grep -c "INCORRECT_DATA"

echo "--- a gap between the slabs of the record variables"
write_file gap "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)" 2>&1 | grep -c "INCORRECT_DATA"

echo "--- the packed layout reads every record back"
write_file packed "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT a, b FROM file('$FILE', NetCDF)"

rm -f "$FILE"
