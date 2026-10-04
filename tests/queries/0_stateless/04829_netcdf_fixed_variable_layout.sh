#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# Writes files that exercise the layout of the fixed-size variables. The fixed cases hold two
# fixed-size variables of eight bytes each, whose data either overlaps (the same offset, or a
# partial overlap), which has to be rejected, or sits side by side, which is the valid layout.
# The streaming cases hold a fixed-size variable next to a record variable of a streaming file
# (the number of records comes from the file size): the fixed-size data either extends past the
# beginning of the records, which has to be rejected, or ends exactly there, which is valid.
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

if kind.startswith('fixed'):
    begin_b = {'fixed-same-offset': 116, 'fixed-partial-overlap': 120, 'fixed-adjacent': 124}[kind]

    header = b'CDF\x01' + tag(0)
    header += tag(NC_DIMENSION) + tag(1) + name('x') + tag(2)
    header += ABSENT
    header += tag(NC_VARIABLE) + tag(2)
    header += name('a') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(8) + tag(116)
    header += name('b') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(8) + tag(begin_b)
    assert len(header) == 116, len(header)

    data = tag(1) + tag(2) + tag(3) + tag(4)
else:
    begin_r = {'into-records': 132, 'streaming-valid': 136}[kind]

    header = b'CDF\x01' + tag(-1)
    header += tag(NC_DIMENSION) + tag(2) + name('x') + tag(2) + name('t') + tag(0)
    header += ABSENT
    header += tag(NC_VARIABLE) + tag(2)
    header += name('a') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(8) + tag(128)
    header += name('r') + tag(1) + tag(1) + ABSENT + tag(NC_INT) + tag(4) + tag(begin_r)
    assert len(header) == 128, len(header)

    data = tag(1) + tag(2) + tag(5) + tag(6)

with open(path, 'wb') as out:
    out.write(header + data)
PYTHON
}

echo "--- two fixed-size variables whose data begins at the same offset"
write_file fixed-same-offset "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)" 2>&1 | grep -c "INCORRECT_DATA"

echo "--- two fixed-size variables whose data overlaps partially"
write_file fixed-partial-overlap "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)" 2>&1 | grep -c "INCORRECT_DATA"

echo "--- two fixed-size variables side by side read back correctly"
write_file fixed-adjacent "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT a, b FROM file('$FILE', NetCDF)"

echo "--- a fixed-size variable extending into the records of a streaming file"
write_file into-records "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)" 2>&1 | grep -c "INCORRECT_DATA"

echo "--- a streaming file whose fixed-size variable ends at the records reads back correctly"
write_file streaming-valid "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(a), sum(r) FROM file('$FILE', NetCDF)"

rm -f "$FILE"
