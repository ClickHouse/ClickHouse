#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# The `-Strings` JSON variants (`JSONStringsEachRow`, `JSONCompactStringsEachRow`) re-parse the
# content of every string value with the whole-text deserializer of the destination type, and
# `Array` / `Tuple` / `Map` all implement whole-text parsing (through `SimpleTextSerialization`),
# so a string like `"[1,2]"` is a valid value for an `Array(UInt8)` column there. Such values stay
# `String` during schema inference, so the diagnostic must not treat them as a structure mismatch
# for a nested destination: a genuine parse error in an unrelated column must not pick up a
# misleading "structure mismatch" explanation. In the typed-token JSON formats a string token for a
# nested column is a genuine mismatch and stays flagged.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- JSONStringsEachRow, array in a string into Array plus an unrelated fractional value for a numeric column (no false positive)"
printf 'CREATE TABLE t (a Array(UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONStringsEachRow\n{"a": "[1,2]", "n": "1.5"}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONCompactStringsEachRow, array in a string into Array plus an unrelated fractional value for a numeric column (no false positive)"
printf 'CREATE TABLE t (a Array(UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONCompactStringsEachRow\n["[1,2]", "1.5"]\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONStringsEachRow, tuple and map in strings plus an unrelated fractional value for a numeric column (no false positive)"
printf 'CREATE TABLE t (t Tuple(UInt8, UInt8), m Map(String, UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONStringsEachRow\n{"t": "(1,2)", "m": "{'"'"'k'"'"':1}", "n": "1.5"}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONStringsEachRow, genuinely non-numeric text into a numeric column (explanation still fires)"
printf 'CREATE TABLE t (a Array(UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONStringsEachRow\n{"a": "[1,2]", "n": "abc"}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow, array as a string token into Array (genuine mismatch, still flagged)"
printf 'CREATE TABLE t (a Array(UInt8)) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"a": "[1,2]"}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check
