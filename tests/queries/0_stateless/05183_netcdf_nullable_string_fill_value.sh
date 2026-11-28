#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# A NULL of a String column is written as the `_FillValue` of the `char` variable, so a NULL and an
# empty string stay different values in the file.
echo "--- a NULL and an empty string are not the same value"
$CLICKHOUSE_LOCAL -q "SELECT if(number = 0, NULL, if(number = 1, '', 'hello')) AS s FROM numbers(3) INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(s), s IS NULL, hex(s) FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

echo "--- without the setting the sentinel is an ordinary value"
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(s), hex(s) FROM file('$FILE', NetCDF)"

echo "--- the sentinel avoids the data of the column"
$CLICKHOUSE_LOCAL -q "SELECT if(number = 0, NULL, char(1)) AS s FROM numbers(2) INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "SELECT s IS NULL, hex(s) FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

echo "--- a FixedString of one byte whose data takes the shortest sentinel"
$CLICKHOUSE_LOCAL -q "SELECT if(number = 0, NULL, toFixedString(char(1), 1)) AS s FROM numbers(2) INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "SELECT s IS NULL, hex(s) FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

echo "--- a String column with no NULLs is written without the attribute"
$CLICKHOUSE_LOCAL -q "SELECT toNullable('hello') AS s FROM numbers(2) INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(s), s FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

rm -f "$FILE"
