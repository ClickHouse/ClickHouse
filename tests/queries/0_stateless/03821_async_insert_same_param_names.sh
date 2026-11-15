#!/usr/bin/env bash
# Test that concurrent async inserts with same param names but different values
# don't cross-contaminate data

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_async_params"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_async_params (x UInt64) ENGINE = MergeTree ORDER BY x"

# Send concurrent inserts with same param name, different values
for i in {1..10}; do
$CLICKHOUSE_CLIENT --param_val=$i --async_insert=1 --wait_for_async_insert=0 \
   -q "INSERT INTO test_async_params VALUES ({val:UInt64})" &
done
wait

# Verify each value appears exactly once
$CLICKHOUSE_CLIENT -q "SELECT x, count() as c FROM test_async_params GROUP BY x HAVING c != 1"
# Should output nothing (all counts are 1)

$CLICKHOUSE_CLIENT -q "DROP TABLE test_async_params"

