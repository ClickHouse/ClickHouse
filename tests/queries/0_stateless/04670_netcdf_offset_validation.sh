#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# Writes malformed files whose headers are valid but whose data offsets are not, plus the one valid
# edge case among them: a streaming file with zero records ends exactly where its record section
# begins.
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

NC_DIMENSION, NC_VARIABLE, NC_INT, ABSENT = 10, 11, 4, tag(0) + tag(0)

kind, path = sys.argv[1], sys.argv[2]

if kind in ('empty', 'past-eof'):
    # One unlimited dimension and one record variable of four bytes, in the streaming mode, so the
    # number of records is derived from the size of the file. The header is 80 bytes and the file
    # ends right after it: a variable that begins at 80 has zero records, and a variable that
    # begins at 100 declares a record section past the end of the file.
    begin = 80 if kind == 'empty' else 100
    header = b'CDF\x01' + struct.pack('>I', 0xFFFFFFFF)
    header += tag(NC_DIMENSION) + tag(1) + name('t') + tag(0)
    header += ABSENT
    header += tag(NC_VARIABLE) + tag(1)
    header += name('v') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(4) + tag(begin)
    assert len(header) == 80, len(header)
    data = header
elif kind == 'into-header':
    # A scalar variable whose data begins at the offset 0, inside the 64-byte header, so reading it
    # would return the magic bytes of the file as an Int32 value.
    header = b'CDF\x01' + tag(0)
    header += ABSENT
    header += ABSENT
    header += tag(NC_VARIABLE) + tag(1)
    header += name('v') + tag(0) + ABSENT + tag(NC_INT) + tag(4) + tag(0)
    assert len(header) == 64, len(header)
    data = header + b'\x00' * 4
else:
    # Two record variables of four bytes each, so a record is eight bytes, but the second variable
    # places its slab at the offset 12 of the record, past its end.
    header = b'CDF\x01' + tag(1)
    header += tag(NC_DIMENSION) + tag(1) + name('t') + tag(0)
    header += ABSENT
    header += tag(NC_VARIABLE) + tag(2)
    header += name('a') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(4) + tag(116)
    header += name('b') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(4) + tag(128)
    assert len(header) == 116, len(header)
    data = header + b'\x00' * 16

with open(path, 'wb') as out:
    out.write(data)
PYTHON
}

echo "--- a streaming file that ends where its record section begins has zero records"
write_file empty "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF)"

echo "--- a streaming file whose record section begins past the end of the file"
write_file past-eof "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)" 2>&1 | grep -c "INCORRECT_DATA"

echo "--- a variable that begins inside the header"
write_file into-header "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)" 2>&1 | grep -c "INCORRECT_DATA"

echo "--- a record variable whose slab does not fit inside the record"
write_file slab-overlap "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)" 2>&1 | grep -c "INCORRECT_DATA"

rm -f "$FILE"
