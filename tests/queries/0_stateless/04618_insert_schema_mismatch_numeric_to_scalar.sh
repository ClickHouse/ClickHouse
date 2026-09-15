#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# The schema-mismatch diagnostic must not attach a misleading "structure mismatch" explanation when a
# numeric value that schema inference widened to `Int64` / `Float64` is inserted into a scalar destination
# that the real parser accepts it into even though the two types share no common supertype: an integer is
# read into `DateTime` / `Date` as a Unix timestamp, into an `Enum` by its numeric value, into `Decimal`,
# and so on. Here the parse fails only because of an unrelated fractional value in another column, not
# because the structures disagree.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- JSONEachRow, integer into DateTime (a Unix timestamp) plus an unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (ts DateTime, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"ts": 1, "n": 1.5}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow, integer into Date plus an unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (d Date, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"d": 1, "n": 1.5}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow, integer into an Enum (by its numeric value) plus an unrelated bad numeric column (no false positive)"
printf "CREATE TABLE t (e Enum8('a' = 1, 'b' = 2), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{\"e\": 1, \"n\": 1.5}\n" \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- TSV, integer into an Array destination is a genuine structure mismatch (a scalar cannot build a nested value)"
printf 'CREATE TABLE t (a Array(UInt8)) ENGINE = Memory; INSERT INTO t FORMAT TSV\n1\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check
