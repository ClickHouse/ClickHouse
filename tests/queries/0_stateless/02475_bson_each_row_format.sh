#!/usr/bin/env bash
# Tags: no-debug

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# In case of parallel parsing and small block
# (--min_chunk_bytes_for_parallel_parsing) we may have multiple blocks, and
# this will break sorting order, so let's limit number of threads to avoid
# reordering.
CLICKHOUSE_CLIENT+="--allow_repeated_settings --max_threads 1"

echo "Integers"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select number::Bool as bool, number::Int8 as int8, number::UInt8 as uint8, number::Int16 as int16, number::UInt16 as uint16, number::Int32 as int32, number::UInt32 as uint32, number::Int64 as int64, number::UInt64 as uint64 from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'bool Bool, int8 Int8, uint8 UInt8, int16 Int16, uint16 UInt16, int32 Int32, uint32 UInt32, int64 Int64, uint64 UInt64')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"

echo "Integers conversion"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'uint64 UInt64, int64 Int64') select 4294967297, -4294967297 settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'uint64 UInt32, int64 UInt32')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'uint64 Int32, int64 Int32')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'uint64 UInt16, int64 UInt16')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'uint64 Int16, int64 Int16')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'uint64 UInt8, int64 UInt8')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'uint64 Int8, int64 Int8')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "Floats"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'float32 Float32, float64 Float64') select number / (number + 1), number / (number + 1) from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'float32 Float32, float64 Float64')";


$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "Big integers"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'int128 Int128, uint128 UInt128, int256 Int256, uint256 UInt256') select number * -10000000000000000000000::Int128 as int128, number * 10000000000000000000000::UInt128 as uint128, number * -100000000000000000000000000000000000000000000::Int256 as int256, number * 100000000000000000000000000000000000000000000::UInt256 as uint256 from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'int128 Int128, uint128 UInt128, int256 Int256, uint256 UInt256')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"


echo "Dates"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'date Date, date32 Date32, datetime DateTime(\'UTC\'), datetime64 DateTime64(6, \'UTC\')') select number, number, number, number from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'date Date, date32 Date32, datetime DateTime(\'UTC\'), datetime64 DateTime64(6, \'UTC\')')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "Decimals"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'decimal32 Decimal32(3), decimal64 Decimal64(6), decimal128 Decimal128(12), decimal256 Decimal256(24)') select number * 42.422::Decimal32(3) as decimal32, number * 42.424242::Decimal64(6) as decimal64, number * 42.424242424242::Decimal128(12) as decimal128, number * 42.424242424242424242424242::Decimal256(24) as decimal256 from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'decimal32 Decimal32(3), decimal64 Decimal64(6), decimal128 Decimal128(12), decimal256 Decimal256(24)')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"


echo "Strings"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'str String, fixstr FixedString(5)') select repeat('HelloWorld', number), repeat(char(97 + number), number % 6) from numbers(5) settings engine_file_truncate_on_insert=1, output_format_bson_string_as_string=0"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'str String, fixstr FixedString(5)')"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'str String, fixstr FixedString(5)') select repeat('HelloWorld', number), repeat(char(97 + number), number % 6) from numbers(5) settings engine_file_truncate_on_insert=1, output_format_bson_string_as_string=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'str String, fixstr FixedString(5)')"


$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "UUID"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'uuid UUID') select 'b86d5c23-4b87-4465-8f33-4a685fa1c868'::UUID settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'uuid UUID')"


$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "LowCardinality"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'lc LowCardinality(String)') select char(97 + number % 3)::LowCardinality(String) from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'lc LowCardinality(String)')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "Nullable"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'null Nullable(UInt32)') select number % 2 ? NULL : number from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'null Nullable(UInt32)')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'null UInt32')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'null UInt32') settings input_format_null_as_default=0" 2>&1 | grep -q -F "ILLEGAL_COLUMN" && echo "OK" || echo "FAIL"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "LowCardinality(Nullable)"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'lc LowCardinality(Nullable(String))') select number % 2 ? NULL : char(97 + number % 3)::LowCardinality(String) from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'lc LowCardinality(Nullable(String))')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "Array"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'arr1 Array(UInt64), arr2 Array(String)') select range(number), ['Hello'] from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'arr1 Array(UInt64), arr2 Array(String)') settings engine_file_truncate_on_insert=1" 

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "Tuple"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'tuple Tuple(x UInt64, s String)') select tuple(number, 'Hello') from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'tuple Tuple(x UInt64, s String)')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'tuple Tuple(s String, x UInt64)')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'tuple Tuple(x UInt64)')" 2>&1 | grep -q -F "INCORRECT_DATA" && echo "OK" || echo "FAIL"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'tuple Tuple(x UInt64, b String)')" 2>&1 | grep -q -F "INCORRECT_DATA" && echo "OK" || echo "FAIL"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'tuple Tuple(UInt64, String)') select tuple(number, 'Hello') from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'tuple Tuple(x UInt64, s String)')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'tuple Tuple(UInt64, String)')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'tuple Tuple(UInt64)')" 2>&1 | grep -q -F "INCORRECT_DATA" && echo "OK" || echo "FAIL"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'tuple Tuple(UInt64, String, UInt64)')" 2>&1 | grep -q -F "INCORRECT_DATA" && echo "OK" || echo "FAIL"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "Map"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'map Map(UInt64, UInt64)') select map(1, number, 2, number + 1) from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'map Map(UInt64, UInt64)')"

$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'map Map(String, UInt64)') select map('a', number, 'b', number + 1) from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'map Map(String, UInt64)')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "Nested types"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'nested1 Array(Array(UInt32)), nested2 Tuple(Tuple(x UInt32, s String), String), nested3 Map(String, Map(String, UInt32))') select [range(number), range(number + 1)], tuple(tuple(number, 'Hello'), 'Hello'), map('a', map('a.a', number, 'a.b', number + 1), 'b', map('b.a', number, 'b.b', number + 1)) from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'nested1 Array(Array(UInt32)), nested2 Tuple(Tuple(x UInt32, s String), String), nested3 Map(String, Map(String, UInt32))')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"

$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'nested Array(Tuple(Map(String, Array(UInt32)), Array(Map(String, Tuple(Array(UInt64), Array(UInt64))))))') select [(map('a', range(number), 'b', range(number + 1)), [map('c', (range(number), range(number + 1))), map('d', (range(number + 2), range(number + 3)))])] from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'nested Array(Tuple(Map(String, Array(UInt32)), Array(Map(String, Tuple(Array(UInt64), Array(UInt64))))))')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "Schema inference"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select number::Bool as x from numbers(2) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select number::Int32 as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select number::UInt32 as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select number::Int64 as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select toString(number) as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)" 2>&1 | grep -q -F "TYPE_MISMATCH" && echo "OK" || echo "FAIL"

$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select [number::Bool] as x from numbers(2) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select [number::Int32] as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select [number::UInt32] as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select [number::Int64] as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select [toString(number)] as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)" 2>&1 | grep -q -F "TYPE_MISMATCH" && echo "OK" || echo "FAIL"

$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select [] as x from numbers(2) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)" 2>&1 | grep -q -F "ONLY_NULLS_WHILE_READING_SCHEMA" && echo "OK" || echo "FAIL"

$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select NULL as x from numbers(2) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)" 2>&1 | grep -q -F "ONLY_NULLS_WHILE_READING_SCHEMA" && echo "OK" || echo "FAIL"

$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select [NULL, 1] as x from numbers(2) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)" 2>&1 | grep -q -F "ONLY_NULLS_WHILE_READING_SCHEMA" && echo "OK" || echo "FAIL"

$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select tuple(1, 'str') as x from numbers(2) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select tuple(1) as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)" 2>&1 | grep -q -F "TYPE_MISMATCH" && echo "OK" || echo "FAIL"


echo "Sync after error"
$CLICKHOUSE_CLIENT -q "insert into function file(data.bsonEachRow) select number, 42::Int128 as int, range(number) as arr from numbers(3) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q " insert into function file(data.bsonEachRow) select number, 'Hello' as int, range(number) as arr from numbers(2) settings engine_file_truncate_on_insert=0"
$CLICKHOUSE_CLIENT -q "insert into function file(data.bsonEachRow) select number, 42::Int128 as int, range(number) as arr from numbers(3) settings engine_file_truncate_on_insert=0"
$CLICKHOUSE_CLIENT -q "select * from file(data.bsonEachRow, auto, 'number UInt64, int Int128, arr Array(UInt64)') settings input_format_allow_errors_num=0"  2>&1 | grep -q -F "INCORRECT_DATA" && echo "OK" || echo "FAIL"
$CLICKHOUSE_CLIENT -q "select * from file(data.bsonEachRow, auto, 'number UInt64, int Int128, arr Array(UInt64)') settings input_format_allow_errors_num=2"
