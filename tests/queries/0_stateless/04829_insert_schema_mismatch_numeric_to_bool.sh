#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A numeric value going into a `Bool` destination: where the parser re-parses the field with the
# `Bool` deserializers (the typed-token JSON formats and the flat-text formats), only the literal
# tokens `1` / `0` (and the word forms) are accepted, so `2` is a genuine structure mismatch the
# schema-mismatch diagnostic must report — the widened inferred type (`Int64` for both `1` and `2`)
# cannot tell them apart, so the diagnostic inspects the sampled values. A column holding only
# `0` / `1` must not produce a false positive, and a custom `bool_true_representation` that
# legitimizes another numeric literal must suppress the check.
#
# In the no-false-positive cases the second column holds a fractional value for a `UInt8` column,
# which produces the genuine parse error that triggers the diagnostic.
# `bool_true_representation` is not randomized by the test harness, so the explicit setting below
# needs no `--allow_repeated_settings`.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- JSONEachRow: a numeric value other than 0/1 for a Bool column is a genuine structure mismatch"
{
    echo "CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow"
    echo '{"b":2,"n":1.5}'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow: the numeric literal 1 for a Bool column is valid (no false positive)"
{
    echo "CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow"
    echo '{"b":1,"n":1.5}'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- TSV: a numeric value other than 0/1 for a Bool column is a genuine structure mismatch"
{
    echo "CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV"
    printf '2\t1.5\n'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- TSV: the numeric literal 0 for a Bool column is valid (no false positive)"
{
    echo "CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV"
    printf '0\t1.5\n'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- TSV: a bad value in a later row is still a structure mismatch"
{
    echo "CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV"
    printf '1\t1\n2\t1\n'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- TSV: a custom bool_true_representation legitimizes the value (no false positive)"
{
    echo "CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t SETTINGS bool_true_representation = '2' FORMAT TSV"
    printf '2\t1.5\n'
} | $CLICKHOUSE_LOCAL 2>&1 | check
