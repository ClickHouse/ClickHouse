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

def variable(name_, type_, attributes, begin):
    result = name(name_) + tag(1) + tag(0)
    result += tag(NC_ATTRIBUTE) + tag(len(attributes)) + b''.join(attributes)
    return result + tag(type_) + tag({NC_CHAR: 1, NC_SHORT: 2, NC_INT: 4}[type_] * 3) + tag(begin)

unsigned = [attribute('_Unsigned', NC_CHAR, 4, b'true')]
missing = [attribute('missing_value', NC_INT, 2, struct.pack('>ii', -9999, -8888))]
char_missing = [attribute('_FillValue', NC_CHAR, 1, b'X')]

header = b'CDF\x01' + tag(0)
header += tag(NC_DIMENSION) + tag(1) + name('x') + tag(3) + ABSENT
header += tag(NC_VARIABLE) + tag(3)
header += variable('unsigned_short', NC_SHORT, unsigned, 0)
header += variable('value', NC_INT, missing, 0)
header += variable('quality_control', NC_CHAR, char_missing, 0)

unsigned_begin = len(header)
value_begin = unsigned_begin + 8
quality_control_begin = value_begin + 12
header = b'CDF\x01' + tag(0)
header += tag(NC_DIMENSION) + tag(1) + name('x') + tag(3) + ABSENT
header += tag(NC_VARIABLE) + tag(3)
header += variable('unsigned_short', NC_SHORT, unsigned, unsigned_begin)
header += variable('value', NC_INT, missing, value_begin)
header += variable('quality_control', NC_CHAR, char_missing, quality_control_begin)

with open(sys.argv[1], 'wb') as out:
    out.write(header)
    out.write(struct.pack('>HHH', 65535, 1, 2))
    out.write(b'\x00\x00')
    out.write(struct.pack('>iii', -9999, -8888, 7))
    out.write(b'XOX')
PYTHON

echo "--- an unsigned classic variable is read as an unsigned type"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF)" | cut -f1,2
$CLICKHOUSE_LOCAL -q "SELECT unsigned_short FROM file('$FILE', NetCDF) ORDER BY unsigned_short"

echo "--- numeric and char missing-value sentinels are read as NULL"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1" | cut -f1,2
$CLICKHOUSE_LOCAL -q "SELECT countIf(value IS NULL), min(value) FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"
$CLICKHOUSE_LOCAL -q "SELECT countIf(quality_control IS NULL), min(quality_control) FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

rm -f "$FILE"
