#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# Writes the result of the query to a file and reads it back, printing the structure and the data.
function roundtrip()
{
    local read_settings=${2:-1}
    $CLICKHOUSE_LOCAL -q "$1 INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
    head -c 4 "$FILE" | od -An -c | tr -s ' '
    $CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE') SETTINGS input_format_netcdf_fill_value_as_null = $read_settings"
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE') SETTINGS input_format_netcdf_fill_value_as_null = $read_settings"
    rm -f "$FILE"
}

echo "--- numbers"
roundtrip "SELECT number::Int8 AS i8, number::Int16 AS i16, number::Int32 AS i32, number::Int64 AS i64, (number / 4)::Float32 AS f32, (number / 8)::Float64 AS f64 FROM numbers(3)" 0

echo "--- unsigned numbers switch the file to CDF-5"
roundtrip "SELECT number::UInt8 AS u8, number::UInt16 AS u16, number::UInt32 AS u32, number::UInt64 AS u64 FROM numbers(3)" 0

echo "--- strings"
roundtrip "SELECT ['a', 'bb', 'ccc'][number + 1] AS s, toFixedString('xy', 2) AS fs FROM numbers(3)" 0

echo "--- NULLs"
roundtrip "SELECT if(number = 1, NULL, number)::Nullable(Int32) AS n, if(number = 2, NULL, toString(number)) AS s FROM numbers(3)"

echo "--- dates and times"
roundtrip "SELECT toDate('2020-01-02') + number AS d, toDate32('1950-01-02') + number AS d32, toDateTime('2020-01-02 03:04:05', 'UTC') + number AS dt, toDateTime64('2020-01-02 03:04:05.123', 3, 'UTC') AS dt64 FROM numbers(2)" 0

echo "--- enums and low cardinality"
roundtrip "SELECT CAST(if(number = 0, 'a', 'b'), 'Enum8(\'a\' = 1, \'b\' = 2)') AS e, toLowCardinality(number::Int32) AS lc FROM numbers(2)" 0

echo "--- an empty result"
roundtrip "SELECT number::Int32 AS x FROM numbers(0)" 0

echo "--- many rows"
$CLICKHOUSE_LOCAL -q "SELECT number AS n, number % 7 AS m FROM numbers(100000) INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(n), sum(m) FROM file('$FILE')"
rm -f "$FILE"

echo "--- errors"
$CLICKHOUSE_LOCAL -q "SELECT [1, 2] AS a FORMAT NetCDF" 2>&1 | grep -c "ILLEGAL_COLUMN"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS \`a/b\` FORMAT NetCDF" 2>&1 | grep -c "BAD_ARGUMENTS"
