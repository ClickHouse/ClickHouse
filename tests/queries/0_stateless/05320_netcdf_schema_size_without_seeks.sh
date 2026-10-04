#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# The header of a truncated NetCDF file is well-formed but promises data that the file does not
# contain. Schema inference checks the header against the size of the file, and it needs only the
# size, not random access, so the check must not be skipped with `input_format_allow_seeks = 0`:
# otherwise `DESCRIBE` accepts (and caches) the schema of a file that `SELECT` rejects.
#
# The file below is a CDF-1 file with the dimension `x` of the length 3 and the variable `int v(x)`,
# truncated right after the header.
python3 - "$FILE" <<'PYTHON'
import struct
import sys

def tag(value):
    return struct.pack('>i', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

NC_DIMENSION, NC_VARIABLE, NC_INT, ABSENT = 10, 11, 4, tag(0) + tag(0)

def header(begin_of_v):
    result = b'CDF\x01' + tag(0)
    result += tag(NC_DIMENSION) + tag(1) + name('x') + tag(3)
    result += ABSENT
    result += tag(NC_VARIABLE) + tag(1)
    result += name('v') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(12) + tag(begin_of_v)
    return result

with open(sys.argv[1], 'wb') as out:
    out.write(header(len(header(0))))
PYTHON

for allow_seeks in 1 0
do
    echo "--- input_format_allow_seeks = $allow_seeks"
    $CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_allow_seeks = $allow_seeks" 2>&1 |
        grep -o -m1 "does not fit in the NetCDF file"
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF) SETTINGS input_format_allow_seeks = $allow_seeks" 2>&1 |
        grep -o -m1 "does not fit in the NetCDF file"
done

rm -f "$FILE"
