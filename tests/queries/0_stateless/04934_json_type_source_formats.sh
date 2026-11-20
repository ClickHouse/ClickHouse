#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Round-trip of the source subcolumn through Native and RowBinary formats.

JSON_TYPE="JSON(with_source=1)"
DATA='{"b" : "Hello",  "a" : 42}'

echo "Native (structured)"
$CLICKHOUSE_LOCAL -q "SELECT '$DATA'::$JSON_TYPE AS json FORMAT Native" \
    | $CLICKHOUSE_LOCAL --input-format=Native --structure="json $JSON_TYPE" -q "SELECT json, json.__source FROM table"

echo "Native (JSON as string)"
$CLICKHOUSE_LOCAL --output_format_native_write_json_as_string=1 -q "SELECT '$DATA'::$JSON_TYPE AS json FORMAT Native" \
    | $CLICKHOUSE_LOCAL --input-format=Native --structure="json $JSON_TYPE" -q "SELECT json, json.__source FROM table"

echo "Native (flattened)"
$CLICKHOUSE_LOCAL --output_format_native_use_flattened_dynamic_and_json_serialization=1 -q "SELECT '$DATA'::$JSON_TYPE AS json FORMAT Native" \
    | $CLICKHOUSE_LOCAL --input-format=Native --structure="json $JSON_TYPE" -q "SELECT json, json.__source FROM table"

echo "RowBinary (structured input, source is created from the object)"
$CLICKHOUSE_LOCAL -q "SELECT '$DATA'::$JSON_TYPE AS json FORMAT RowBinary" \
    | $CLICKHOUSE_LOCAL --input-format=RowBinary --structure="json $JSON_TYPE" -q "SELECT json, json.__source FROM table"

echo "RowBinary (JSON as string on both sides, original text is preserved)"
$CLICKHOUSE_LOCAL --output_format_binary_write_json_as_string=1 --output_format_json_type_use_source=1 -q "SELECT '$DATA'::$JSON_TYPE AS json FORMAT RowBinary" \
    | $CLICKHOUSE_LOCAL --input_format_binary_read_json_as_string=1 --input-format=RowBinary --structure="json $JSON_TYPE" -q "SELECT json, json.__source FROM table"

echo "RowBinary (typed paths that are not in the data)"
$CLICKHOUSE_LOCAL -q "SELECT '{\"b\" : 1}'::JSON AS json FORMAT RowBinary" \
    | $CLICKHOUSE_LOCAL --input-format=RowBinary --structure="json JSON(with_source=1, s String, arr Array(UInt32))" -q "SELECT json, json.__source FROM table"

echo "A reserved path in Native input is rejected"
$CLICKHOUSE_LOCAL -q "SELECT '{\"__source\" : 42}'::JSON AS json FORMAT Native" \
    | $CLICKHOUSE_LOCAL --input-format=Native --structure="json $JSON_TYPE" -q "SELECT json FROM table" 2>&1 | grep -c "INCORRECT_DATA"

echo "A reserved key in RowBinary input is rejected"
$CLICKHOUSE_LOCAL --output_format_binary_write_json_as_string=0 -q "SELECT '{\"__source\" : 42}'::JSON AS json FORMAT RowBinary" \
    | $CLICKHOUSE_LOCAL --input-format=RowBinary --structure="json $JSON_TYPE" -q "SELECT json FROM table" 2>&1 | grep -c "INCORRECT_DATA"

echo "A path nested in the reserved name in Native input is rejected"
$CLICKHOUSE_LOCAL -q "SELECT '{\"__source\" : {\"a\" : 42}}'::JSON AS json FORMAT Native" \
    | $CLICKHOUSE_LOCAL --input-format=Native --structure="json $JSON_TYPE" -q "SELECT json FROM table" 2>&1 | grep -c "INCORRECT_DATA"

echo "A flattened Native block with a truncated source is rejected"
$CLICKHOUSE_LOCAL --output_format_native_use_flattened_dynamic_and_json_serialization=1 -q "SELECT '{}'::JSON(with_source=1) AS json FORMAT Native" | head -c -3 \
    | $CLICKHOUSE_LOCAL --input-format=Native --structure="json $JSON_TYPE" -q "SELECT count() FROM table" 2>&1 | grep -c "INCORRECT_DATA"

echo "A path nested in the reserved name in RowBinary input is rejected"
$CLICKHOUSE_LOCAL --output_format_binary_write_json_as_string=0 -q "SELECT '{\"__source\" : {\"a\" : 42}}'::JSON AS json FORMAT RowBinary" \
    | $CLICKHOUSE_LOCAL --input-format=RowBinary --structure="json $JSON_TYPE" -q "SELECT json FROM table" 2>&1 | grep -c "INCORRECT_DATA"

echo "A reserved path in the shared data of a Native block is rejected"
# Write a block whose paths include '__sourcX', over max_dynamic_paths so one goes to the shared data,
# then rewrite the byte-equal name to the reserved '__source'.
$CLICKHOUSE_LOCAL -q "SELECT materialize('{\"a\" : 1, \"__sourcX\" : 2}')::JSON(with_source=1, max_dynamic_paths=1) AS json FROM numbers(3) FORMAT Native" \
    | python3 -I -c "import sys; b=sys.stdin.buffer.read(); sys.stdout.buffer.write(b.replace(b'__sourcX', b'__source'))" \
    | $CLICKHOUSE_LOCAL --input-format=Native --structure="json JSON(with_source=1, max_dynamic_paths=1)" -q "SELECT json FROM table" 2>&1 | grep -c "INCORRECT_DATA"
