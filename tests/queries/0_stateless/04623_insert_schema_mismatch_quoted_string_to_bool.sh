#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# A quoted string into a `Bool` column is rejected by `SerializationBool::deserializeTextJSON`
# (`CANNOT_PARSE_BOOL`) — the `input_format_json_read_bools_as_strings` setting covers the opposite
# direction, a boolean token into a `String` column — and the schema-mismatch diagnostic explains it:
# `Bool` is backed by a `UInt8`, so a confirmed-text `String` inferred for it hits the reliable
# "text where a number is expected" rule.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- JSONEachRow, quoted string into Bool (genuine mismatch)"
printf 'CREATE TABLE t (b Bool) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"b": "true"}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow, boolean token into Bool plus an unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"b": true, "n": 1.5}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check
