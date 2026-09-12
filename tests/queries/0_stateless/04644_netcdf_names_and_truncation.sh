#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

echo "--- a value equal to the default fill value stays a value"
# The writer has to pick a value to write the NULLs as that the data does not contain, or reading
# the file back would turn -2147483647, which is the fill value of the netCDF library for an int,
# into a NULL.
$CLICKHOUSE_LOCAL -q "SELECT if(number = 0, NULL, -2147483647)::Nullable(Int32) AS n FROM numbers(2) INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE') SETTINGS input_format_netcdf_fill_value_as_null = 1"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE') SETTINGS input_format_netcdf_fill_value_as_null = 1"
rm -f "$FILE"

echo "--- the same for a Float64 column"
$CLICKHOUSE_LOCAL -q "SELECT if(number = 0, NULL, 9.9692099683868690e+36)::Nullable(Float64) AS f FROM numbers(2) INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE') SETTINGS input_format_netcdf_fill_value_as_null = 1"
rm -f "$FILE"

echo "--- a name that the classic format cannot store"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS \`-temp\` FORMAT NetCDF" 2>&1 | grep -c "BAD_ARGUMENTS"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS \`temp \` FORMAT NetCDF" 2>&1 | grep -c "BAD_ARGUMENTS"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS \`.temp\` FORMAT NetCDF" 2>&1 | grep -c "BAD_ARGUMENTS"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS \`_temp.1\` FORMAT NetCDF" > /dev/null && echo "a valid name is accepted"

echo "--- a name has to be NFC-normalized"
$CLICKHOUSE_LOCAL -q $'SELECT 1 AS `e\u0301` FORMAT NetCDF' > /dev/null 2>&1 && exit 1
echo "a non-NFC name is rejected"

if [ "$( ${CLICKHOUSE_LOCAL} -q "SELECT value FROM system.build_options WHERE name = 'USE_ICU' LIMIT 1")" = "1" ]; then
    $CLICKHOUSE_LOCAL -q $'SELECT 1 AS `\u00e9` FORMAT NetCDF' > /dev/null
fi
echo "NFC validation is enforced"

# Writes the files that the cases below need. They cannot be produced by the netCDF library: one of
# them is truncated, and the other one has a variable that shares the name of a dimension without
# being its coordinate variable.
write_file()
{
    python3 - "$1" "$2" <<'PYTHON'
import struct
import sys

def tag(value):
    return struct.pack('>i', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

NC_DIMENSION, NC_VARIABLE, NC_INT, ABSENT = 10, 11, 4, tag(0) + tag(0)

kind, path = sys.argv[1], sys.argv[2]

if kind.startswith('streaming'):
    # One unlimited dimension and one record variable of four bytes. The number of records is
    # written as all ones, which means that a reader has to derive it from the size of the file.
    header = b'CDF\x01' + struct.pack('>I', 0xFFFFFFFF)
    header += tag(NC_DIMENSION) + tag(1) + name('t') + tag(0)
    header += ABSENT
    header += tag(NC_VARIABLE) + tag(1)
    header += name('v') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(4) + tag(80)
    assert len(header) == 80, len(header)
    records = b''.join(struct.pack('>i', value) for value in range(3))
    data = header + (records if kind == 'streaming' else records[:-2])
else:
    # A dimension named x and a scalar variable of the same name, which is not its coordinate
    # variable, so the index along the dimension is not available anywhere else.
    header = b'CDF\x01' + tag(0)
    header += tag(NC_DIMENSION) + tag(1) + name('x') + tag(2)
    header += ABSENT
    header += tag(NC_VARIABLE) + tag(2)
    header += name('v') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(8) + tag(112)
    header += name('x') + tag(0) + ABSENT + tag(NC_INT) + tag(4) + tag(120)
    assert len(header) == 112, len(header)
    data = header + struct.pack('>iii', 10, 20, 7)

with open(path, 'wb') as out:
    out.write(data)
PYTHON
}

echo "--- a streaming file with a whole number of records"
write_file streaming "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)"

echo "--- a streaming file that is truncated in the middle of a record"
write_file streaming-truncated "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)" 2>&1 | grep -c "INCORRECT_DATA"
rm -f "$FILE"

echo "--- a variable that shares the name of a dimension without being its coordinate variable"
write_file collision "$FILE"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 1"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_add_dimension_columns = 1"
rm -f "$FILE"
