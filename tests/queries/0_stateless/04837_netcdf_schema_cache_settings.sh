#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# A CDF-1 file with the dimension `x`, which has no coordinate variable of its own, and the single
# variable `int v(x)` with a `_FillValue` attribute. Both settings of the format change the schema
# that is inferred from this file: `input_format_netcdf_add_dimension_columns` adds the column `x`
# with the index along the dimension, and `input_format_netcdf_fill_value_as_null` makes the column
# `v` Nullable. So a schema inferred with one value of a setting must never be served from the
# schema cache to a query that has the other value.
python3 - "$FILE" <<'PYTHON'
import struct
import sys

def tag(value):
    return struct.pack('>i', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

NC_DIMENSION, NC_VARIABLE, NC_ATTRIBUTE, NC_INT, ABSENT = 10, 11, 12, 4, tag(0) + tag(0)

def header(begin_of_v):
    result = b'CDF\x01' + tag(0)
    result += tag(NC_DIMENSION) + tag(1) + name('x') + tag(3)
    result += ABSENT
    result += tag(NC_VARIABLE) + tag(1)
    result += name('v') + tag(1) + tag(0)
    result += tag(NC_ATTRIBUTE) + tag(1) + name('_FillValue') + tag(NC_INT) + tag(1) + tag(-1)
    result += tag(NC_INT) + tag(12) + tag(begin_of_v)
    return result

size = len(header(0))

with open(sys.argv[1], 'wb') as out:
    out.write(header(size) + struct.pack('>iii', 1, -1, 3))
PYTHON

# The schema cache treats an entry as stale when the file was modified at or after the time the
# entry was registered, and both happen in the same second here, so the file has to be backdated
# for the cache to be used at all.
touch -d '2020-01-01 00:00:00' "$FILE"

# Every group of queries below runs in one process, so the later queries of a group see the schema
# that the first one put into the cache.

echo "--- input_format_netcdf_add_dimension_columns, on and then off"
$CLICKHOUSE_LOCAL -m -q "
DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 1;
DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 0;
SELECT * FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 0;
"

echo "--- input_format_netcdf_add_dimension_columns, off and then on"
$CLICKHOUSE_LOCAL -m -q "
DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 0;
DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 1;
SELECT * FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 1;
"

echo "--- input_format_netcdf_fill_value_as_null, on and then off"
$CLICKHOUSE_LOCAL -m -q "
DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1;
DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 0;
SELECT * FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 0;
"

echo "--- input_format_netcdf_fill_value_as_null, off and then on"
$CLICKHOUSE_LOCAL -m -q "
DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 0;
DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1;
SELECT * FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1;
"

rm -f "$FILE"
