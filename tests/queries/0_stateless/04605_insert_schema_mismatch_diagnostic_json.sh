#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# Regression tests for the schema-mismatch diagnostic on formats that read values from text and do not
# have a strict column order (here: JSONEachRow).
#
# 1) On the clickhouse-client path the destination is a remote table not registered in the local catalog.
#    Schema inference used to resolve it through the insertion table to reorder columns, which threw and
#    silently suppressed the whole diagnostic, so a genuine structure mismatch showed only the bare parse
#    error. The diagnostic must fire for such formats too, and it must handle columns whose order in the
#    data differs from the table.
# 2) Schema inference keeps JSON string values as `String` even for columns whose real parser accepts them
#    from text (`UUID`, `IPv4` / `IPv6`, `Enum`, `FixedString`, dates). A parse error caused by an
#    unrelated column must not pick up a misleading "structure mismatch" suffix in that case.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_mismatch_json (a Int64, b Int64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_mismatch_json_rich (id UUID, n UInt8) ENGINE = Memory"

echo "-- JSONEachRow via the client, genuine structure mismatch"
printf 'INSERT INTO test_mismatch_json FORMAT JSONEachRow\n{"a": "hello", "b": "world"}\n' \
    | $CLICKHOUSE_CLIENT --async_insert 0 2>&1 | check

echo "-- JSONEachRow, columns reordered relative to the table, still a genuine mismatch"
printf 'INSERT INTO test_mismatch_json FORMAT JSONEachRow\n{"b": 1, "a": "not_a_number"}\n' \
    | $CLICKHOUSE_CLIENT --async_insert 0 2>&1 | check

echo "-- JSONEachRow, valid UUID string but a fractional value for a UInt8 column (no false positive)"
printf 'INSERT INTO test_mismatch_json_rich FORMAT JSONEachRow\n{"id": "d9428888-122b-11e1-b85c-61cd3cbb3210", "n": 1.5}\n' \
    | $CLICKHOUSE_CLIENT --async_insert 0 2>&1 | check

$CLICKHOUSE_CLIENT -q "DROP TABLE test_mismatch_json"
$CLICKHOUSE_CLIENT -q "DROP TABLE test_mismatch_json_rich"
