#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_contradictory.nc

# A valid file can have variables whose dimension orders contradict each other: `a` is over (x, y)
# and `b` is over (y, x). The order of the rows agrees with `a`, so the values that one chunk of
# rows needs from `b` are spread over almost the whole variable, and reading them as one contiguous
# range would take memory proportional to the size of the variable. The memory limit of the queries
# below is well under the size of `b`, so they pass only when the reader collects the needed values
# instead of buffering the range. The netCDF library always writes the data of a variable in the
# order of its dimensions, so the file is written here.
python3 - "$FILE" <<'PYTHON'
import struct
import sys
from array import array

NC_DIMENSION, NC_VARIABLE, NC_BYTE, NC_DOUBLE = 10, 11, 1, 6
ABSENT = struct.pack('>ii', 0, 0)
N = 2048


def tag(value):
    return struct.pack('>i', value)


def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)


# a(x, y) = 7 everywhere; b(y, x) = x * N + y, so the value of `b` differs from its position in the
# file and a read that maps the values to the wrong rows is caught by the checks below.
a_data = b'\x07' * (N * N)
b_parts = []
for y in range(N):
    values = array('d', range(y, y + N * N, N))
    if sys.byteorder == 'little':
        values.byteswap()
    b_parts.append(values.tobytes())
b_data = b''.join(b_parts)


def header(a_begin, b_begin):
    result = b'CDF\x01' + tag(0)
    result += tag(NC_DIMENSION) + tag(2) + name('x') + tag(N) + name('y') + tag(N)
    result += ABSENT
    result += tag(NC_VARIABLE) + tag(2)
    result += name('a') + tag(2) + tag(0) + tag(1) + ABSENT + tag(NC_BYTE) + tag(len(a_data)) + tag(a_begin)
    result += name('b') + tag(2) + tag(1) + tag(0) + ABSENT + tag(NC_DOUBLE) + tag(len(b_data)) + tag(b_begin)
    return result


offset = len(header(0, 0))
with open(sys.argv[1], 'wb') as out:
    out.write(header(offset, offset + len(a_data)) + a_data + b_data)
PYTHON

$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 1" | cut -f1,2

$CLICKHOUSE_LOCAL -q "SELECT count(), sum(b) FROM file('$FILE', NetCDF) SETTINGS max_memory_usage = 30000000"

$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF) WHERE b != x * 2048 + y OR a != 7
    SETTINGS input_format_netcdf_add_dimension_columns = 1, max_memory_usage = 30000000"

$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF) LIMIT 3
    SETTINGS input_format_netcdf_add_dimension_columns = 1, max_memory_usage = 30000000"

rm -f "$FILE"
