#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc
rm -f "$FILE"

# The classic format has only signed byte, short, and int types. `_Unsigned = "true"` makes the
# stored bits unsigned, and `missing_value` may contain more than one value.
python3 - "$FILE" <<'PYTHON'
import struct
import sys

NC_DIMENSION, NC_VARIABLE, NC_ATTRIBUTE = 10, 11, 12
NC_CHAR, NC_SHORT, NC_INT = 2, 3, 4
ABSENT = struct.pack('>ii', 0, 0)

def tag(value):
    return struct.pack('>i', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

def attribute(name_, type_, num_elements, values):
    return name(name_) + tag(type_) + tag(num_elements) + values + b'\x00' * ((4 - len(values) % 4) % 4)

def variable(name_, type_, dimensions, attributes, data_size, begin):
    result = name(name_) + tag(len(dimensions)) + b''.join(tag(dimension) for dimension in dimensions)
    result += tag(NC_ATTRIBUTE) + tag(len(attributes)) + b''.join(attributes)
    return result + tag(type_) + tag(data_size) + tag(begin)

unsigned = [attribute('_Unsigned', NC_CHAR, 4, b'true')]
missing = [attribute('missing_value', NC_INT, 2, struct.pack('>ii', -9999, -8888))]
char_fill_value = [attribute('_FillValue', NC_CHAR, 2, b'NA')]
char_missing_value = [attribute('missing_value', NC_CHAR, 2, b'NA')]

header = b'CDF\x01' + tag(0)
header += tag(NC_DIMENSION) + tag(2) + name('x') + tag(3) + name('nchar') + tag(2) + ABSENT
header += tag(NC_VARIABLE) + tag(4)
header += variable('unsigned_short', NC_SHORT, [0], unsigned, 6, 0)
header += variable('value', NC_INT, [0], missing, 12, 0)
header += variable('quality_control_fill', NC_CHAR, [0, 1], char_fill_value, 6, 0)
header += variable('quality_control_missing', NC_CHAR, [0, 1], char_missing_value, 6, 0)

unsigned_begin = len(header)
value_begin = unsigned_begin + 8
quality_control_fill_begin = value_begin + 12
quality_control_missing_begin = quality_control_fill_begin + 8
header = b'CDF\x01' + tag(0)
header += tag(NC_DIMENSION) + tag(2) + name('x') + tag(3) + name('nchar') + tag(2) + ABSENT
header += tag(NC_VARIABLE) + tag(4)
header += variable('unsigned_short', NC_SHORT, [0], unsigned, 6, unsigned_begin)
header += variable('value', NC_INT, [0], missing, 12, value_begin)
header += variable('quality_control_fill', NC_CHAR, [0, 1], char_fill_value, 6, quality_control_fill_begin)
header += variable('quality_control_missing', NC_CHAR, [0, 1], char_missing_value, 6, quality_control_missing_begin)

with open(sys.argv[1], 'wb') as out:
    out.write(header)
    out.write(struct.pack('>HHH', 65535, 1, 2))
    out.write(b'\x00\x00')
    out.write(struct.pack('>iii', -9999, -8888, 7))
    out.write(b'NAN\x00OK\x00\x00')
    out.write(b'NAA\x00OK\x00\x00')
PYTHON

echo "--- an unsigned classic variable is read as an unsigned type"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF)" | cut -f1,2
$CLICKHOUSE_LOCAL -q "SELECT unsigned_short FROM file('$FILE', NetCDF) ORDER BY unsigned_short"

echo "--- numeric and string missing-value sentinels are read as NULL"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1" | cut -f1,2
$CLICKHOUSE_LOCAL -q "SELECT countIf(value IS NULL), min(value) FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"
$CLICKHOUSE_LOCAL -q "SELECT countIf(quality_control_fill IS NULL), min(quality_control_fill) FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"
$CLICKHOUSE_LOCAL -q "SELECT countIf(quality_control_missing IS NULL), min(quality_control_missing) FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

rm -f "$FILE"
