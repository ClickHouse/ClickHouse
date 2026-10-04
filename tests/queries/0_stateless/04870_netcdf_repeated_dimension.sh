#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}

# A variable may use the same dimension more than once, as in `correlation(instrument,
# instrument)`, and every use is an axis of the row space of its own. Two files pin the mapping:
# one where the coordinate variable `instrument` provides the values along the first use and the
# index along the second use comes as the column `instrument_index`, and one where a variable uses
# the same dimension three times and no coordinate variable exists, so the index columns are named
# `t`, `t_index` and `t_index_2`. The netCDF library writes such files, but building them here
# keeps the byte layout of the test stable.
python3 - "$PREFIX" <<'PYTHON'
import struct
import sys

NC_DIMENSION, NC_VARIABLE, NC_INT = 10, 11, 4
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

# A correlation matrix over the same dimension twice, with a coordinate variable and another
# variable over the first use of the dimension. correlation[i][j] = 10 * i + j.
with open(prefix + '_matrix.nc', 'wb') as out:
    out.write(build(
        [('instrument', 3)],
        [('instrument', [0], NC_INT, struct.pack('>3i', 100, 101, 102)),
         ('correlation', [0, 0], NC_INT, struct.pack('>9i', *[10 * i + j for i in range(3) for j in range(3)])),
         ('gain', [0], NC_INT, struct.pack('>3i', 7, 14, 21))]))

# The same dimension three times and no coordinate variable. triple[a][b][c] = 4*a + 2*b + c, which
# is the position of the value in the file, so a wrong axis mapping is caught.
with open(prefix + '_triple.nc', 'wb') as out:
    out.write(build(
        [('t', 2)],
        [('triple', [0, 0, 0], NC_INT, struct.pack('>8i', *range(8)))]))
PYTHON

echo '--- matrix'
$CLICKHOUSE_LOCAL -q "DESCRIBE file('${PREFIX}_matrix.nc', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 1" | cut -f1,2
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('${PREFIX}_matrix.nc', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 1"
$CLICKHOUSE_LOCAL -q "
    SELECT count(), countIf(correlation != 10 * (instrument - 100) + instrument_index)
    FROM file('${PREFIX}_matrix.nc', NetCDF)
    SETTINGS input_format_netcdf_add_dimension_columns = 1"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('${PREFIX}_matrix.nc', NetCDF)"

echo '--- triple'
$CLICKHOUSE_LOCAL -q "DESCRIBE file('${PREFIX}_triple.nc', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 1" | cut -f1,2
$CLICKHOUSE_LOCAL -q "
    SELECT count(), countIf(triple != 4 * t + 2 * t_index + t_index_2)
    FROM file('${PREFIX}_triple.nc', NetCDF)
    SETTINGS input_format_netcdf_add_dimension_columns = 1"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('${PREFIX}_triple.nc', NetCDF)"

rm -f "${PREFIX}_matrix.nc" "${PREFIX}_triple.nc"
