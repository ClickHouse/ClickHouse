#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# The `_FillValue` attribute of a variable must be scalar - the netCDF library refuses to fill a
# variable whose `_FillValue` has more than one element - so the NULLs of a String column are
# written as a single character other than the zero byte that the data of the column does not
# contain as a one-byte string, and the reader rejects a `_FillValue` of several characters on a
# `char` variable that is not exposed as strings.

echo "--- the sentinel is the next free one-byte string"
$CLICKHOUSE_LOCAL -q "SELECT if(number = 3, NULL, char(number + 1)) AS s FROM numbers(4) INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "SELECT hex(s) FROM file('$FILE', NetCDF)"
$CLICKHOUSE_LOCAL -q "SELECT s IS NULL, hex(s) FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

echo "--- a longer string of the candidate byte does not take the candidate"
$CLICKHOUSE_LOCAL -q "SELECT if(number = 0, NULL, char(1, 1)) AS s FROM numbers(2) INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "SELECT hex(s) FROM file('$FILE', NetCDF)"

echo "--- 254 one-byte strings and a NULL: the one left over is the sentinel"
$CLICKHOUSE_LOCAL -q "SELECT if(number = 200, NULL, char(number + 1)) AS s FROM numbers(255) INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "SELECT hex(s) FROM file('$FILE', NetCDF) WHERE rowNumberInAllBlocks() = 200"
$CLICKHOUSE_LOCAL -q "SELECT countIf(s IS NULL), uniqExact(s) FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

echo "--- all of the 255 one-byte strings and a NULL: nothing is left"
$CLICKHOUSE_LOCAL -q "SELECT if(number = 255, NULL, char(number + 1)) AS s FROM numbers(256) INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF" 2>&1 | grep -c "BAD_ARGUMENTS"

echo "--- the same 255 strings without a NULL need no sentinel"
$CLICKHOUSE_LOCAL -q "SELECT toNullable(char(number + 1)) AS s FROM numbers(255) INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(s), count() FROM file('$FILE', NetCDF) GROUP BY 1 SETTINGS input_format_netcdf_fill_value_as_null = 1"

# A file of another writer: `char v(x)` next to `int w(x)` holds one character per row, because
# `w` keeps `x` in the row space, and `char v(x, nchar)` holds strings. The attribute
# `_FillValue = "NA"` has two elements either way.
write_file()
{
    python3 - "$FILE" "$1" "$2" <<'PYTHON'
import struct
import sys

def tag(value):
    return struct.pack('>i', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

NC_DIMENSION, NC_ATTRIBUTE, NC_VARIABLE, NC_CHAR, NC_INT = 10, 12, 11, 2, 4
ABSENT = tag(0) + tag(0)

path, attribute_name, with_strings = sys.argv[1], sys.argv[2], sys.argv[3] == 'strings'
rows = [b'NA', b'NB', b'N'] if with_strings else [b'N', b'A', b'B']
v_data = b''.join(row.ljust(2, b'\x00') if with_strings else row for row in rows)
v_size = len(v_data)
v_data += b'\x00' * ((4 - v_size % 4) % 4)
w_data = struct.pack('>iii', 1, 2, 3)

def header(v_begin, w_begin):
    result = b'CDF\x01' + tag(0)
    if with_strings:
        result += tag(NC_DIMENSION) + tag(2) + name('x') + tag(3) + name('nchar') + tag(2)
    else:
        result += tag(NC_DIMENSION) + tag(1) + name('x') + tag(3)
    result += ABSENT
    result += tag(NC_VARIABLE) + tag(1 if with_strings else 2)
    result += name('v') + (tag(2) + tag(0) + tag(1) if with_strings else tag(1) + tag(0))
    result += tag(NC_ATTRIBUTE) + tag(1) + name(attribute_name) + tag(NC_CHAR) + tag(2) + b'NA\x00\x00'
    result += tag(NC_CHAR) + tag(v_size) + tag(v_begin)
    if not with_strings:
        result += name('w') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(len(w_data)) + tag(w_begin)
    return result

v_begin = len(header(0, 0))
w_begin = v_begin + len(v_data)

with open(path, 'wb') as out:
    out.write(header(v_begin, w_begin))
    out.write(v_data)
    if not with_strings:
        out.write(w_data)
PYTHON
}

echo "--- a two-character _FillValue of a variable of characters is rejected"
write_file _FillValue characters
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1" 2>&1 | grep -c "INCORRECT_DATA"
$CLICKHOUSE_LOCAL -q "SELECT v, w FROM file('$FILE', NetCDF)"

echo "--- a two-character missing_value of a variable of characters is two sentinels"
write_file missing_value characters
$CLICKHOUSE_LOCAL -q "SELECT v, w FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

echo "--- a two-character _FillValue of a variable of strings is one string"
write_file _FillValue strings
$CLICKHOUSE_LOCAL -q "SELECT v FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

rm -f "$FILE"
