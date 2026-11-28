#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# The file has the dimensions `x` and `x_index` and a variable named `x` that is not the coordinate
# variable of `x`, so both dimensions need a column with the index along them, and the name of the
# first one cannot be the name of the second one. The netCDF library cannot write this file.
python3 - "$FILE" <<'PYTHON'
import struct
import sys

def tag(value):
    return struct.pack('>i', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

NC_DIMENSION, NC_VARIABLE, NC_INT, ABSENT = 10, 11, 4, tag(0) + tag(0)

def header(begin_of_v, begin_of_x):
    result = b'CDF\x01' + tag(0)
    result += tag(NC_DIMENSION) + tag(2) + name('x') + tag(2) + name('x_index') + tag(3)
    result += ABSENT
    result += tag(NC_VARIABLE) + tag(2)
    result += name('v') + tag(2) + tag(0) + tag(1) + ABSENT + tag(NC_INT) + tag(24) + tag(begin_of_v)
    result += name('x') + tag(0) + ABSENT + tag(NC_INT) + tag(4) + tag(begin_of_x)
    return result

size = len(header(0, 0))
data = header(size, size + 24) + struct.pack('>iiiiii', 10, 11, 12, 20, 21, 22) + struct.pack('>i', 7)

with open(sys.argv[1], 'wb') as out:
    out.write(data)
PYTHON

$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 1"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 1"

rm -f "$FILE"
