#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# Writes files with two record variables of four bytes each. Their slabs inside the record either
# overlap (the same offset, or a partial overlap), which has to be rejected, or sit side by side,
# which is the valid layout.
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

begin_b = {'same-offset': 116, 'partial-overlap': 118, 'adjacent': 120}[kind]

header = b'CDF\x01' + tag(1)
header += tag(NC_DIMENSION) + tag(1) + name('t') + tag(0)
header += ABSENT
header += tag(NC_VARIABLE) + tag(2)
header += name('a') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(4) + tag(116)
header += name('b') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(4) + tag(begin_b)
assert len(header) == 116, len(header)

with open(path, 'wb') as out:
    out.write(header + tag(1) + tag(2))
PYTHON
}

echo "--- two record variables whose slabs begin at the same offset of the record"
write_file same-offset "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)" 2>&1 | grep -c "INCORRECT_DATA"

echo "--- two record variables whose slabs overlap partially"
write_file partial-overlap "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)" 2>&1 | grep -c "INCORRECT_DATA"

echo "--- two record variables whose slabs sit side by side read back correctly"
write_file adjacent "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT a, b FROM file('$FILE', NetCDF)"

rm -f "$FILE"
