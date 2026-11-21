#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# The schema-mismatch diagnostic must not attach a misleading "structure mismatch" explanation when the
# real parse error is unrelated to the structure. Schema inference widens types (e.g. numbers with a
# fractional part are inferred as `Float64`) and does not reconstruct wrappers such as `LowCardinality`,
# so a destination column of `UInt8` / `Int32` / `LowCardinality(String)` is still compatible with the
# sampled input even though the inferred type name differs. Here the parse fails only because a fractional
# value cannot be read into an integer column, not because the structures disagree.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- narrower numeric column (UInt8): compatible with the sampled numeric input, fails only on a fractional value"
printf 'CREATE TABLE t (x UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV\n1.5\n2.5\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- LowCardinality(String) plus a narrower numeric column: both compatible, fails only on the fractional number"
printf 'CREATE TABLE t (s LowCardinality(String), n Int32) ENGINE = Memory; INSERT INTO t FORMAT TSV\nhello\t1.5\nworld\t2.5\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check
