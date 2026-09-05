#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc
rm -f "$FILE"

# The value that marks a NULL is searched for below the preferred one, and the data here occupies a
# dense range that begins exactly there, so the search has to walk past every value of the column
# before it finds a free one. It is a single pass over the data, so the size of that range does not
# make the search any slower.

echo "--- a dense range below the preferred value of UInt16"
$CLICKHOUSE_LOCAL -q "
    SELECT if(number = 60000, NULL, toUInt16(65535 - number)) AS x
    FROM numbers(60001) INTO OUTFILE '$FILE' FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "
    SELECT count(), countIf(x IS NULL), min(x), max(x)
    FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

echo "--- the same range in UInt32, where the free value is far from the preferred one"
rm -f "$FILE"
$CLICKHOUSE_LOCAL -q "
    SELECT if(number = 100000, NULL, toUInt32(4294967295 - number)) AS x
    FROM numbers(100001) INTO OUTFILE '$FILE' FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "
    SELECT count(), countIf(x IS NULL), min(x), max(x)
    FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

rm -f "$FILE"
