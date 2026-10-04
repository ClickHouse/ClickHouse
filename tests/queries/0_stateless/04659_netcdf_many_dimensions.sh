#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# The parser reserves room for 1024 dimensions and the list of dimensions grows past that, so a
# file with more dimensions exercises the reallocation of the list, and the duplicate-name check
# must survive it. The first file has 1500 distinct dimensions, the second repeats the name of the
# first dimension in the last one.
generate()
{
    python3 - "$FILE" "$1" <<'PYTHON'
import struct
import sys

def tag(value):
    return struct.pack('>i', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

NC_DIMENSION, NC_VARIABLE, NC_INT, ABSENT = 10, 11, 4, tag(0) + tag(0)
NUM_DIMENSIONS = 1500
last_dimension_name = 'd0' if sys.argv[2] == 'duplicate' else f'd{NUM_DIMENSIONS - 1}'

def header(begin):
    result = b'CDF\x01' + tag(0)
    result += tag(NC_DIMENSION) + tag(NUM_DIMENSIONS)
    for i in range(NUM_DIMENSIONS - 1):
        result += name(f'd{i}') + tag(3)
    result += name(last_dimension_name) + tag(3)
    result += ABSENT
    result += tag(NC_VARIABLE) + tag(1)
    result += name('x') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(12) + tag(begin)
    return result

size = len(header(0))

with open(sys.argv[1], 'wb') as out:
    out.write(header(size) + struct.pack('>iii', 10, 20, 30))
PYTHON
}

generate distinct
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF)"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)"

generate duplicate
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)" 2>&1 | grep -c 'more than one dimension named'

rm -f "$FILE"
