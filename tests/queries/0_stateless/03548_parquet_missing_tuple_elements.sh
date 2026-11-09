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

# The arrays of a missing element have the sizes of the arrays in the file.
$CHL -q "select 'missing with dot, alone:', * from file('$file', Parquet, '\`x.z\` Array(String)')"
$CHL -q "select 'missing with dot, beside one:', * from file('$file', Parquet, '\`x.y\` Array(String), \`x.z\` Array(String)')"
$CHL -q "select 'array join:', count() from file('$file', Parquet, '\`x.z\` Array(String)') array join \`x.z\`"
$CHL -q "select 'where:', count() from file('$file', Parquet, '\`x.z\` Array(String)') where length(\`x.z\`) > 0 settings optimize_move_to_prewhere = 0"
$CHL -q "select 'prewhere:', count() from file('$file', Parquet, '\`x.z\` Array(String)') prewhere length(\`x.z\`) > 0"
$CLICKHOUSE_LOCAL -q "select * from file('$file', Parquet, '\`x.z\` Array(String)') settings input_format_parquet_allow_missing_columns = 0" 2>&1 | grep -o -m1 'Code: [0-9]*'

nested_file=${CLICKHOUSE_TEST_UNIQUE_NAME}_nested.parquet
$CHL -q "insert into function file('$nested_file', Parquet) select [[(1, 'a')], []]::Array(Array(Tuple(x Int64, y String))) as n"
$CHL -q "select 'nested arrays, all elements missing:', * from file('$nested_file', Parquet, 'n Array(Array(Tuple(z String)))')"
$CHL -q "select 'nested arrays, missing with dot:', * from file('$nested_file', Parquet, '\`n.z\` Array(Array(String))')"

struct_file=${CLICKHOUSE_TEST_UNIQUE_NAME}_struct.parquet
$CHL -q "insert into function file('$struct_file', Parquet, 's Array(Tuple(t Tuple(a Int64)))') select arrayMap(i -> tuple(tuple(number * 10 + i)), range(number)) from numbers(3)"
$CHL -q "select 'tuple in element, all elements missing:', * from file('$struct_file', Parquet, 's Array(Tuple(t Tuple(b String)))')"
$CHL -q "select 'tuple in element, missing with dot:', * from file('$struct_file', Parquet, '\`s.t.b\` Array(String)')"
$CHL -q "select 'tuple in element with dot, all elements missing:', * from file('$struct_file', Parquet, '\`s.t\` Array(Tuple(b String))')"

rm $file $nested_file $struct_file
