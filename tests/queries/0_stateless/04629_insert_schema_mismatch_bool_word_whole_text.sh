#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# The `-Strings` JSON variants (`JSONStringsEachRow`, `JSONCompactStringsEachRow`) re-parse the
# content of every string value with the whole-text deserializer of the destination type, and
# `SerializationBool::deserializeWholeText` accepts the word forms (`"true"` / `"false"`) as well as
# the quoted numerics. The word forms stay `String` in the number-from-string inference pass, so the
# diagnostic must not treat them as "text where a number is expected" for the `UInt8`-backed `Bool`
# destination: a genuine parse error in an unrelated column must not pick up a misleading
# "structure mismatch" explanation.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- JSONStringsEachRow, bool word into Bool plus an unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONStringsEachRow\n{"b": "true", "n": "1.5"}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONCompactStringsEachRow, bool word into Bool plus an unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONCompactStringsEachRow\n["false", "1.5"]\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONStringsEachRow, genuinely non-numeric text into a numeric column (explanation still fires)"
printf 'CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONStringsEachRow\n{"b": "true", "n": "abc"}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow, bool word as a string token into Bool (genuine mismatch, still flagged)"
printf 'CREATE TABLE t (b Bool) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"b": "true"}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check
