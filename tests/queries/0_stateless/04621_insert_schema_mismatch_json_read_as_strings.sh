#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# The typed-token JSON formats accept a bare number / boolean / array / object token into a `String`
# column only under the corresponding `input_format_json_read_*_as_strings` setting (all enabled by
# default). The schema-mismatch diagnostic must follow the parser: with a setting disabled, such a token
# into a `String` destination is a genuine structure mismatch and deserves the explanation; with the
# defaults, it parses fine and an unrelated parse error elsewhere in the row must not pick up a
# misleading explanation. The settings are JSON-specific: `TSV` reads every field verbatim into a
# `String` column regardless, so no explanation may appear there either.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- JSONEachRow, number into String with input_format_json_read_numbers_as_strings = 0 (genuine mismatch)"
printf 'CREATE TABLE t (s String) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"s": 1}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_read_numbers_as_strings 0 2>&1 | check

echo "-- JSONEachRow, number into String with the setting enabled plus an unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"s": 1, "n": 1.5}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_read_numbers_as_strings 1 2>&1 | check

echo "-- JSONEachRow, boolean into String with input_format_json_read_bools_as_strings = 0 (genuine mismatch)"
printf 'CREATE TABLE t (s String) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"s": true}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_read_bools_as_strings 0 2>&1 | check

echo "-- JSONEachRow, boolean into String with the setting enabled plus an unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"s": true, "n": 1.5}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_read_bools_as_strings 1 2>&1 | check

echo "-- JSONEachRow, array into String with input_format_json_read_arrays_as_strings = 0 (genuine mismatch)"
printf 'CREATE TABLE t (s String) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"s": [1, 2]}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_read_arrays_as_strings 0 2>&1 | check

echo "-- JSONEachRow, array into String with the setting enabled plus an unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"s": [1, 2], "n": 1.5}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_read_arrays_as_strings 1 2>&1 | check

echo "-- JSONEachRow, object into String with input_format_json_read_objects_as_strings = 0 (genuine mismatch)"
printf 'CREATE TABLE t (s String) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"s": {"a": 1}}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_read_objects_as_strings 0 2>&1 | check

echo "-- JSONEachRow, object into String with the setting enabled plus an unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"s": {"a": 1}, "n": 1.5}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_read_objects_as_strings 1 2>&1 | check

echo "-- TSV, number into String with input_format_json_read_numbers_as_strings = 0 plus an unrelated bad numeric column (JSON-only setting, no false positive)"
printf 'CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV\n1\t1.5\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_read_numbers_as_strings 0 2>&1 | check
