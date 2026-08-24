#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/79887
# Regression test: a rejected oversized ALTER on a Buffer table must not flush
# buffered rows into the destination table.  Before the fix, StorageBuffer::alter
# called optimize() (flush) before the max_query_size check in alterTable, so the
# rejected ALTER still mutated user-visible data placement.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS buffer_04538"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS dest_04538"

# Columns with long names so the CREATE statement exceeds a small max_query_size.
cols=$($CLICKHOUSE_CLIENT -q "SELECT arrayStringConcat(arrayMap(i -> 'column_with_a_long_name_' || toString(i) || ' Int8', range(30)), ', ')")
$CLICKHOUSE_CLIENT -q "CREATE TABLE dest_04538 (${cols}) ENGINE = Memory"

# The time/rows/bytes thresholds are all set high enough that the background flush
# thread never fires during the test, so the only thing that can move rows into
# dest_04538 is an (erroneous) flush from StorageBuffer::alter — which is exactly
# what this test checks for. A small max_time here would let the periodic flush
# race the count() checks and make the test flaky.
$CLICKHOUSE_CLIENT -q "CREATE TABLE buffer_04538 (${cols}) ENGINE = Buffer(currentDatabase(), 'dest_04538', 1, 1000000, 1000000, 1000000000, 1000000000, 1000000000, 1000000000)"

# Insert a row into the buffer. It must stay in the buffer because no ALTER has flushed it.
$CLICKHOUSE_CLIENT -q "INSERT INTO buffer_04538 (column_with_a_long_name_0) SELECT 1"

# Sanity: the destination is empty before the ALTER attempt.
echo "dest_before=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM dest_04538")"

# A short MODIFY COMMENT ALTER whose resulting metadata exceeds max_query_size.
# This must fail with QUERY_IS_TOO_LARGE.
$CLICKHOUSE_CLIENT --max_query_size=512 -q "ALTER TABLE buffer_04538 MODIFY COMMENT 'x'" 2>&1 | grep -o -F -m1 "QUERY_IS_TOO_LARGE"

# After the rejected ALTER, the destination must still be empty — the flush in
# StorageBuffer::alter must not have run for a rejected ALTER.
echo "dest_after=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM dest_04538")"

$CLICKHOUSE_CLIENT -q "DROP TABLE buffer_04538"
$CLICKHOUSE_CLIENT -q "DROP TABLE dest_04538"
