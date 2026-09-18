#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}

# The last dimension of a `char` variable is the length of its strings only when nothing else in the
# file needs it as a dimension of the row space. Three files: the dimension of the characters has a
# variable of its own, another variable is over it, and neither, which is the only case where the
# variable is read as one string per row. The netCDF library cannot write the first two files in a
# way that the API of this test can rely on, so they are written here.
python3 - "$PREFIX" <<'PYTHON'
import struct
import sys

NC_DIMENSION, NC_VARIABLE, NC_CHAR, NC_INT = 10, 11, 2, 4
ABSENT = struct.pack('>ii', 0, 0)


def tag(value):
    return struct.pack('>i', value)


def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)


def pad4(data):
    return data + b'\x00' * ((4 - len(data) % 4) % 4)


def build(dimensions, variables):
    """dimensions: [(name, length)]; variables: [(name, [dimension ids], type, data)]"""
    def header(begins):
        result = b'CDF\x01' + tag(0)
        result += tag(NC_DIMENSION) + tag(len(dimensions))
        for dimension_name, length in dimensions:
            result += name(dimension_name) + tag(length)
        result += ABSENT
        result += tag(NC_VARIABLE) + tag(len(variables))
        for (variable_name, dimension_ids, variable_type, data), begin in zip(variables, begins):
            result += name(variable_name) + tag(len(dimension_ids))
            for dimension_id in dimension_ids:
                result += tag(dimension_id)
            result += ABSENT + tag(variable_type) + tag(len(pad4(data))) + tag(begin)
        return result

    begins = []
    offset = len(header([0] * len(variables)))
    for _, _, _, data in variables:
        begins.append(offset)
        offset += len(pad4(data))

    return header(begins) + b''.join(pad4(data) for _, _, _, data in variables)


prefix = sys.argv[1]

# `char station(station)`: the dimension has a variable of its own, so it stays a real axis.
with open(prefix + '_coordinate.nc', 'wb') as out:
    out.write(build([('station', 3)], [('station', [0], NC_CHAR, b'abc')]))

# `nchar` is also a dimension of a numeric variable, so it stays a real axis.
with open(prefix + '_shared.nc', 'wb') as out:
    out.write(build(
        [('station', 2), ('nchar', 3)],
        [('nm', [0, 1], NC_CHAR, b'ab\x00cd\x00'), ('flag', [1], NC_INT, struct.pack('>iii', 1, 2, 3))]))

# Nothing but the last dimension of a `char` variable uses `nchar`: it is the length of the strings.
with open(prefix + '_length.nc', 'wb') as out:
    out.write(build(
        [('station', 2), ('nchar', 3)],
        [('nm', [0, 1], NC_CHAR, b'ab\x00cd\x00')]))
PYTHON

for CASE in coordinate shared length
do
    echo "--- $CASE"
    $CLICKHOUSE_LOCAL -q "DESCRIBE file('${PREFIX}_$CASE.nc', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 1" | cut -f1,2
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('${PREFIX}_$CASE.nc', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 1"
    rm -f "${PREFIX}_$CASE.nc"
done

# The other side of the same rule: the dimension that the writer creates for a string column must not
# take the name of a column, or the string column would be read back as one character per row.
echo "--- written"
$CLICKHOUSE_LOCAL -q "SELECT 'hello' AS s, 1::Int32 AS s_strlen FORMAT NetCDF" > "${PREFIX}_written.nc"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('${PREFIX}_written.nc', NetCDF)"
rm -f "${PREFIX}_written.nc"
