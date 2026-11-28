#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# A CDF-2 file writes all ones as the declared size of a variable that is larger than 4 GiB, and the
# real size is recalculated from the dimensions. The sentinel does not fit into the signed 32-bit
# field, so it must be accepted without validation. The file below is small, but it carries the
# sentinel; writing a real 4 GiB file in a test is not an option.
python3 - "$FILE" <<'PYTHON'
import struct
import sys

def tag(value):
    return struct.pack('>i', value)

def offset(value):
    return struct.pack('>q', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

NC_DIMENSION, NC_VARIABLE, NC_INT, ABSENT = 10, 11, 4, tag(0) + tag(0)

def header(begin):
    result = b'CDF\x02' + tag(0)
    result += tag(NC_DIMENSION) + tag(1) + name('row') + tag(3)
    result += ABSENT
    result += tag(NC_VARIABLE) + tag(1)
    result += name('x') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + b'\xff\xff\xff\xff' + offset(begin)
    return result

size = len(header(0))

with open(sys.argv[1], 'wb') as out:
    out.write(header(size) + struct.pack('>iii', 10, 20, 30))
PYTHON

$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF)"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)"

rm -f "$FILE"
