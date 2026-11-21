#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc
rm -f "$FILE"

# A `Nullable` column that contains no NULLs needs no value to write them as, so the `_FillValue`
# attribute is not written and the column is read back as not `Nullable`, even when the type has
# a value to spare and even with `input_format_netcdf_fill_value_as_null` enabled.

echo "--- a Nullable column with no NULLs: no _FillValue, read back as not Nullable"
$CLICKHOUSE_LOCAL -q "
    SELECT toNullable(toUInt16(number)) AS x
    FROM numbers(2) INTO OUTFILE '$FILE' FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "
    DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1" | cut -f1,2
$CLICKHOUSE_LOCAL -q "
    SELECT x FROM file('$FILE', NetCDF) ORDER BY x SETTINGS input_format_netcdf_fill_value_as_null = 1"

echo "--- the same column with a NULL: the attribute is written and the column stays Nullable"
rm -f "$FILE"
$CLICKHOUSE_LOCAL -q "
    SELECT if(number = 1, NULL, toUInt16(number)) AS x
    FROM numbers(2) INTO OUTFILE '$FILE' FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "
    DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1" | cut -f1,2
$CLICKHOUSE_LOCAL -q "
    SELECT countIf(x IS NULL), min(x)
    FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

echo "--- a NULL in the data, but the file is read without the setting: a plain number"
$CLICKHOUSE_LOCAL -q "
    DESCRIBE file('$FILE', NetCDF)" | cut -f1,2

rm -f "$FILE"
