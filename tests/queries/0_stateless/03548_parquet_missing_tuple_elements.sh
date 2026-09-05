#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

file=${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet
CHL="$CLICKHOUSE_LOCAL --input_format_parquet_allow_missing_columns=1"

$CHL -q "insert into function file('$file', Parquet, 'x Array(Tuple(x Int64, y String))') select arrayMap(i -> (number * 10 + i, toString(number)||'-'||toString(i)), range(number)) from numbers(3)"

$CHL -q "select 'all:', * from file('$file')"
$CHL -q "select 'missing in Tuple:', * from file('$file', Parquet, 'x Array(Tuple(x Int64, z String))')"
$CHL -q "select 'missing with dot:', * from file('$file', Parquet, '\`x.x\` Array(Int64), \`x.y\` Array(String), \`x.z\` Array(String)')"
$CHL -q "select 'all elements missing:', * from file('$file', Parquet, 'x Array(Tuple(z String))')"

# Only the old reader allows this.
$CHL -q "select 'wrong type with dot:', * from file('$file', Parquet, '\`x.x\` Int64') settings input_format_parquet_use_native_reader_v3=0"
$CHL -q "select 'wrong type and missing element with dot:', * from file('$file', Parquet, '\`x.x\` Int64, \`x.y\` Array(String)') settings input_format_parquet_use_native_reader_v3=0"

rm $file
