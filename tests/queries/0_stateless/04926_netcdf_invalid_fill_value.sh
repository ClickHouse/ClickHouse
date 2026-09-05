#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

python3 - "$FILE" <<'PYTHON'
import struct
import sys

def tag(value):
    return struct.pack('>i', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

NC_DIMENSION, NC_ATTRIBUTE, NC_VARIABLE, NC_INT = 10, 12, 11, 4
ABSENT = tag(0) + tag(0)

header = b'CDF\x01' + tag(0)
header += tag(NC_DIMENSION) + tag(1) + name('x') + tag(2)
header += ABSENT
header += tag(NC_VARIABLE) + tag(1)
header += name('v') + tag(1) + tag(0)
header += tag(NC_ATTRIBUTE) + tag(1) + name('_FillValue') + tag(NC_INT) + tag(2) + struct.pack('>ii', -9999, -8888)
header += tag(NC_INT) + tag(8) + tag(112)
assert len(header) == 112, len(header)

with open(sys.argv[1], 'wb') as out:
    out.write(header)
    out.write(struct.pack('>ii', -9999, -8888))
PYTHON

echo "--- a numeric _FillValue must be scalar"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1" 2>&1 | grep -c "INCORRECT_DATA"

rm -f "$FILE"
