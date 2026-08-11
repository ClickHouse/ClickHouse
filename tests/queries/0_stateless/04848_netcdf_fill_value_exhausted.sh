#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc
rm -f "$FILE"

# A NULL is written as the value of the `_FillValue` attribute, which has to be a value that the
# data of the column does not contain, so a small type whose every value is present in the data
# leaves nothing to write the NULLs as. This pins the documented boundary of that contract.

echo "--- all 256 values of UInt8 and a NULL: nothing is left for the _FillValue"
$CLICKHOUSE_LOCAL -q "
    SELECT if(number = 256, NULL, toUInt8(number)) AS x
    FROM numbers(257) INTO OUTFILE '$FILE' FORMAT NetCDF" 2>&1 | grep -c "no value left to write the NULLs as"

echo "--- 255 values of UInt8 and a NULL: the missing value becomes the _FillValue"
rm -f "$FILE"
$CLICKHOUSE_LOCAL -q "
    SELECT if(number = 255, NULL, toUInt8(number)) AS x
    FROM numbers(256) INTO OUTFILE '$FILE' FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "
    SELECT count(), countIf(x IS NULL), min(x), max(x)
    FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

echo "--- a Nullable column of all 256 values but no NULL: written without the attribute"
rm -f "$FILE"
$CLICKHOUSE_LOCAL -q "
    SELECT if(number = 1000, NULL, toUInt8(number)) AS x
    FROM numbers(256) INTO OUTFILE '$FILE' FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "
    SELECT count(), countIf(x IS NULL), min(x), max(x)
    FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

echo "--- a wider type always has a value to spare"
rm -f "$FILE"
$CLICKHOUSE_LOCAL -q "
    SELECT if(number = 256, NULL, toUInt16(number)) AS x
    FROM numbers(257) INTO OUTFILE '$FILE' FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "
    SELECT count(), countIf(x IS NULL)
    FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

rm -f "$FILE"
