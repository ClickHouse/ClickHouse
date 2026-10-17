#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}

# `input_format_netcdf_add_dimension_columns` treats a variable as a coordinate variable when it is
# one-dimensional over the dimension of its name after the trailing string-length dimension of a
# `char` variable is taken as the length of the strings. Two files pin both sides of the rule:
# `char station(station, nchar)` where `nchar` holds the string lengths is a coordinate variable of
# string labels, so no index column is added for `station`; the same variable when another variable
# also uses `nchar` stays two-dimensional, so it is not a coordinate variable and the index column
# is added. The netCDF library cannot write these files in a way that the API of this test can rely
# on, so they are written here.
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

# `nchar` holds the lengths of the strings, so `station` is a coordinate variable of string labels.
with open(prefix + '_labels.nc', 'wb') as out:
    out.write(build(
        [('station', 3), ('nchar', 4)],
        [('station', [0, 1], NC_CHAR, b'AAA\x00BB\x00\x00C\x00\x00\x00'),
         ('temp', [0], NC_INT, struct.pack('>iii', 10, 20, 30))]))

# `nchar` is also a dimension of a numeric variable, so it stays a real axis, `station` is
# two-dimensional, and the index column is added.
with open(prefix + '_shared.nc', 'wb') as out:
    out.write(build(
        [('station', 3), ('nchar', 4)],
        [('station', [0, 1], NC_CHAR, b'AAA\x00BB\x00\x00C\x00\x00\x00'),
         ('temp', [0], NC_INT, struct.pack('>iii', 10, 20, 30)),
         ('code', [1], NC_INT, struct.pack('>iiii', 1, 2, 3, 4))]))
PYTHON

for CASE in labels shared
do
    echo "--- $CASE"
    $CLICKHOUSE_LOCAL -q "DESCRIBE file('${PREFIX}_$CASE.nc', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 1" | cut -f1,2
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('${PREFIX}_$CASE.nc', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 1"
    rm -f "${PREFIX}_$CASE.nc"
done
