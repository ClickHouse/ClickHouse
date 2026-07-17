#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# The schema-mismatch diagnostic must stay conservative for formats with named fields that read values
# from text (here: JSONEachRow). Two cases that must NOT pick up a misleading "structure mismatch" suffix
# on an otherwise unrelated parse error:
#   1) A quoted JSON string for a destination the parser accepts from text but schema inference leaves as
#      `String` (e.g. `Decimal`). Inference reports `String`, the real deserializer reads the value fine,
#      so a genuine parse error in a *different* column must not implicate the structure.
#   2) A row that omits some columns. Named formats default-fill omitted columns, so a shorter row is not
#      a structure mismatch even though the inferred column count differs from the destination.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_mismatch_json_decimal (d Decimal(9, 2), n UInt8) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_mismatch_json_omitted (a UInt8, b String) ENGINE = Memory"

echo "-- Decimal read from a quoted string is compatible; the parse fails only on the fractional UInt8 value"
printf 'INSERT INTO test_mismatch_json_decimal FORMAT JSONEachRow\n{"d": "1.23", "n": 1.5}\n' \
    | $CLICKHOUSE_CLIENT --async_insert 0 2>&1 | check

echo "-- a row that omits a column is default-filled, not a structure mismatch; the parse fails only on the fractional value"
printf 'INSERT INTO test_mismatch_json_omitted FORMAT JSONEachRow\n{"a": 1.5}\n' \
    | $CLICKHOUSE_CLIENT --async_insert 0 2>&1 | check

$CLICKHOUSE_CLIENT -q "DROP TABLE test_mismatch_json_decimal"
$CLICKHOUSE_CLIENT -q "DROP TABLE test_mismatch_json_omitted"
