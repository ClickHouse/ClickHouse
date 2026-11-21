#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# With `input_format_import_nested_json` enabled, the row-based JSON parsers map a top-level object
# field (`{"n": {...}}`) onto the dotted columns of a `Nested` carrier (`n.i`, ...). Schema inference
# reports the top-level field as a single `Tuple` column and cannot represent that mapping, so the
# schema-mismatch diagnostic is skipped in that mode: a parse error on an unrelated column must not
# pick up a misleading explanation just because the nested carrier field does not literally match the
# dotted destination columns. Without the setting, the parser genuinely rejects the unknown top-level
# field (when `input_format_skip_unknown_fields` is disabled) and the explanation is accurate.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- import_nested_json = 1, valid nested object plus an unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (n Nested(i UInt8), x UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"n": {"i": [1]}, "x": 1.5}\n' \
    | $CLICKHOUSE_LOCAL --input_format_import_nested_json 1 --input_format_skip_unknown_fields 0 2>&1 | check

echo "-- import_nested_json = 1, mismatch inside the nested data (diagnostic conservatively skipped)"
printf 'CREATE TABLE t (n Nested(i UInt8)) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"n": {"i": ["abc"]}}\n' \
    | $CLICKHOUSE_LOCAL --input_format_import_nested_json 1 2>&1 | check

echo "-- import_nested_json = 0, the nested carrier field is genuinely unknown (explanation present)"
printf 'CREATE TABLE t (n Nested(i UInt8), x UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"n": {"i": [1]}, "x": 1.5}\n' \
    | $CLICKHOUSE_LOCAL --input_format_import_nested_json 0 --input_format_skip_unknown_fields 0 2>&1 | check
