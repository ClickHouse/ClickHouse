#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# A quoted numeric string (`"1"` / `"0"`) is accepted into a real numeric column, but not into a
# `Bool` one: `SerializationBool::deserializeTextJSON` rejects any string token, and the `CSV` `Bool`
# deserializer reads the raw field without unquoting it. So for a `Bool` destination an inferred
# `String` is a genuine structure mismatch even when the second, number-from-string inference pass
# confirms numeric content. The exception are the `-Strings` JSON variants (`JSONStringsEachRow`,
# `JSONCompactStringsEachRow`), which re-parse the content of every string value with the whole-text
# deserializer of the destination type, where a quoted `"1"` does parse into `Bool`.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- JSONEachRow, quoted numeric string into Bool (genuine mismatch)"
printf 'CREATE TABLE t (b Bool) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"b": "1"}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow, quoted zero into Bool (genuine mismatch)"
printf 'CREATE TABLE t (b Bool) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"b": "0"}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- CSV, quoted numeric string into Bool (genuine mismatch)"
printf 'CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSV\n"1",2\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONStringsEachRow, quoted numeric string into Bool plus an unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONStringsEachRow\n{"b": "1", "n": "1.5"}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONCompactStringsEachRow, quoted numeric string into Bool plus an unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONCompactStringsEachRow\n["1", "1.5"]\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check
