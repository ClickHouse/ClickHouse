#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `aggregate_function_input_format = 'value'` / `'array'`, an `AggregateFunction` column in
# `RowBinaryWithNamesAndTypes` input holds the argument value (or an array of values) instead of a raw
# serialized state. Skipping an unknown `AggregateFunction` column goes through the `Field`-based
# `deserializeBinary`, which must consume exactly the same bytes as the column-based read path,
# otherwise all following columns are misaligned. This test writes files with the header
# `a UInt8, s AggregateFunction(avg, UInt32), b UInt8` and checks that `s` is both readable and
# skippable under both settings, leaving `b` aligned.

VALUE_FILE="${CLICKHOUSE_TMP}/04635_value.rowbinary"
ARRAY_FILE="${CLICKHOUSE_TMP}/04635_array.rowbinary"

HEADER='\x03\x01a\x01s\x01b\x05UInt8\x1eAggregateFunction(avg, UInt32)\x05UInt8'

# Row: a = 1, s = UInt32 value 2, b = 3.
printf "${HEADER}\x01\x02\x00\x00\x00\x03" > "$VALUE_FILE"
# Row: a = 1, s = Array(UInt32) [2, 5], b = 3.
printf "${HEADER}\x01\x02\x02\x00\x00\x00\x05\x00\x00\x00\x03" > "$ARRAY_FILE"

echo '-- read, value'
$CLICKHOUSE_LOCAL -q "SELECT a, finalizeAggregation(s), b FROM file('$VALUE_FILE', 'RowBinaryWithNamesAndTypes', 'a UInt8, s AggregateFunction(avg, UInt32), b UInt8') SETTINGS aggregate_function_input_format = 'value'"
echo '-- skip, value'
$CLICKHOUSE_LOCAL -q "SELECT a, b FROM file('$VALUE_FILE', 'RowBinaryWithNamesAndTypes', 'a UInt8, b UInt8') SETTINGS input_format_skip_unknown_fields = 1, aggregate_function_input_format = 'value'"
echo '-- read, array'
$CLICKHOUSE_LOCAL -q "SELECT a, finalizeAggregation(s), b FROM file('$ARRAY_FILE', 'RowBinaryWithNamesAndTypes', 'a UInt8, s AggregateFunction(avg, UInt32), b UInt8') SETTINGS aggregate_function_input_format = 'array'"
echo '-- skip, array'
$CLICKHOUSE_LOCAL -q "SELECT a, b FROM file('$ARRAY_FILE', 'RowBinaryWithNamesAndTypes', 'a UInt8, b UInt8') SETTINGS input_format_skip_unknown_fields = 1, aggregate_function_input_format = 'array'"

# A `JSON` argument with `input_format_binary_read_json_as_string = 1` is read as a length-prefixed
# string, while the `Field`-based deserialization of `JSON` always reads the structured form, so the
# skip path must go through the column-based read path to consume the same bytes.
JSON_FILE="${CLICKHOUSE_TMP}/04635_json.rowbinary"
JSON_HEADER='\x03\x01a\x01s\x01b\x05UInt8\x1cAggregateFunction(any, JSON)\x05UInt8'
# Row: a = 1, s = JSON string '{"k":1}' (length-prefixed), b = 3.
printf "${JSON_HEADER}\x01\x07{\"k\":1}\x03" > "$JSON_FILE"

echo '-- read, value JSON-as-string'
$CLICKHOUSE_LOCAL -q "SELECT a, finalizeAggregation(s), b FROM file('$JSON_FILE', 'RowBinaryWithNamesAndTypes', 'a UInt8, s AggregateFunction(any, JSON), b UInt8') SETTINGS aggregate_function_input_format = 'value', input_format_binary_read_json_as_string = 1"
echo '-- skip, value JSON-as-string'
$CLICKHOUSE_LOCAL -q "SELECT a, b FROM file('$JSON_FILE', 'RowBinaryWithNamesAndTypes', 'a UInt8, b UInt8') SETTINGS input_format_skip_unknown_fields = 1, aggregate_function_input_format = 'value', input_format_binary_read_json_as_string = 1"

# The default `state` mode uses raw aggregate-state bytes. Verify both the normal read and unknown-column
# skip path against a file written by ClickHouse itself.
STATE_FILE="${CLICKHOUSE_TMP}/04635_state.rowbinary"
$CLICKHOUSE_LOCAL -q "SELECT 1::UInt8 AS a, avgState(2::UInt32) AS s, 3::UInt8 AS b FORMAT RowBinaryWithNamesAndTypes" > "$STATE_FILE"
echo '-- read, state'
$CLICKHOUSE_LOCAL -q "SELECT a, finalizeAggregation(s), b FROM file('$STATE_FILE', 'RowBinaryWithNamesAndTypes', 'a UInt8, s AggregateFunction(avg, UInt32), b UInt8')"
echo '-- skip, state'
$CLICKHOUSE_LOCAL -q "SELECT a, b FROM file('$STATE_FILE', 'RowBinaryWithNamesAndTypes', 'a UInt8, b UInt8') SETTINGS input_format_skip_unknown_fields = 1"

rm -f "$VALUE_FILE" "$ARRAY_FILE" "$JSON_FILE" "$STATE_FILE"
