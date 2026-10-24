#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_row_order.nc

# The order of the rows follows the order of the dimensions of the variables, not the alphabetical
# order of the dimension names that `xarray.Dataset.to_dataframe` defaults to. The file below has
# the variable `v(zz, aa)`, so the rows go over `zz` first and enumerate `aa` fastest, and `v` is
# read back in the order it is stored: 0, 1, 2, 3, 4, 5. The alphabetical order would go over `aa`
# first and produce 0, 3, 1, 4, 2, 5.
python3 - "$FILE" <<'PYTHON'
import struct
import sys

NC_DIMENSION, NC_VARIABLE, NC_INT = 10, 11, 4
ABSENT = struct.pack('>ii', 0, 0)


def tag(value):
    return struct.pack('>i', value)


def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)


def ints(values):
    return struct.pack('>%di' % len(values), *values)


zz_data = ints([10, 20])
aa_data = ints([100, 200, 300])
v_data = ints(range(6))


def header(zz_begin, aa_begin, v_begin):
    result = b'CDF\x01' + tag(0)
    result += tag(NC_DIMENSION) + tag(2) + name('zz') + tag(2) + name('aa') + tag(3)
    result += ABSENT
    result += tag(NC_VARIABLE) + tag(3)
    result += name('zz') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(len(zz_data)) + tag(zz_begin)
    result += name('aa') + tag(1) + tag(1) + ABSENT + tag(NC_INT) + tag(len(aa_data)) + tag(aa_begin)
    result += name('v') + tag(2) + tag(0) + tag(1) + ABSENT + tag(NC_INT) + tag(len(v_data)) + tag(v_begin)
    return result


offset = len(header(0, 0, 0))
with open(sys.argv[1], 'wb') as out:
    out.write(header(offset, offset + len(zz_data), offset + len(zz_data) + len(aa_data)))
    out.write(zz_data + aa_data + v_data)
PYTHON

$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF)" | cut -f1,2

$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)"

rm -f "$FILE"
