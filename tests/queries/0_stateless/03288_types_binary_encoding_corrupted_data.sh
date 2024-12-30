#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select x'0101742080897a' format TSVRaw" | $CLICKHOUSE_LOCAL --input-format RowBinaryWithNamesAndTypes --input_format_binary_decode_types_in_binary_format -q "select * from table" 2>&1 | grep "Too.*INCORRECT_DATA)" -o
$CLICKHOUSE_LOCAL -q "select x'0101741f80897a' format TSVRaw" | $CLICKHOUSE_LOCAL --input-format RowBinaryWithNamesAndTypes --input_format_binary_decode_types_in_binary_format -q "select * from table" 2>&1 | grep "Too.*INCORRECT_DATA)" -o
$CLICKHOUSE_LOCAL -q "select x'0101741780897a' format TSVRaw" | $CLICKHOUSE_LOCAL --input-format RowBinaryWithNamesAndTypes --input_format_binary_decode_types_in_binary_format -q "select * from table" 2>&1 | grep "Too.*INCORRECT_DATA)" -o
$CLICKHOUSE_LOCAL -q "select x'0101742480897a' format TSVRaw" | $CLICKHOUSE_LOCAL --input-format RowBinaryWithNamesAndTypes --input_format_binary_decode_types_in_binary_format -q "select * from table" 2>&1 | grep "Too.*INCORRECT_DATA)" -o
$CLICKHOUSE_LOCAL -q "select x'0101742a80897a' format TSVRaw" | $CLICKHOUSE_LOCAL --input-format RowBinaryWithNamesAndTypes --input_format_binary_decode_types_in_binary_format -q "select * from table" 2>&1 | grep "Too.*INCORRECT_DATA)" -o
$CLICKHOUSE_LOCAL -q "select x'0101742f80897a' format TSVRaw" | $CLICKHOUSE_LOCAL --input-format RowBinaryWithNamesAndTypes --input_format_binary_decode_types_in_binary_format -q "select * from table" 2>&1 | grep "Too.*INCORRECT_DATA)" -o
$CLICKHOUSE_LOCAL -q "select x'01017425000373756d80897a' format TSVRaw" | $CLICKHOUSE_LOCAL --input-format RowBinaryWithNamesAndTypes --input_format_binary_decode_types_in_binary_format -q "select * from table" 2>&1 | grep "Too.*INCORRECT_DATA)" -o
$CLICKHOUSE_LOCAL -q "select x'01017425000373756d0080897a' format TSVRaw" | $CLICKHOUSE_LOCAL --input-format RowBinaryWithNamesAndTypes --input_format_binary_decode_types_in_binary_format -q "select * from table" 2>&1 | grep "Too.*INCORRECT_DATA)" -o
$CLICKHOUSE_LOCAL -q "select x'0101743000010180897a' format TSVRaw" | $CLICKHOUSE_LOCAL --input-format RowBinaryWithNamesAndTypes --input_format_binary_decode_types_in_binary_format -q "select * from table" 2>&1 | grep "Too.*INCORRECT_DATA)" -o
$CLICKHOUSE_LOCAL -q "select x'010174300001010080897a' format TSVRaw" | $CLICKHOUSE_LOCAL --input-format RowBinaryWithNamesAndTypes --input_format_binary_decode_types_in_binary_format -q "select * from table" 2>&1 | grep "Too.*INCORRECT_DATA)" -o
$CLICKHOUSE_LOCAL -q "select x'01017430000101000080897a' format TSVRaw" | $CLICKHOUSE_LOCAL --input-format RowBinaryWithNamesAndTypes --input_format_binary_decode_types_in_binary_format -q "select * from table" 2>&1 | grep "Too.*INCORRECT_DATA)" -o

