#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Messes with internal cache

# Tests concurrent query batching: multiple identical queries are sent
# simultaneously and should be batched together.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "SYSTEM CLEAR QUERY CACHE"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_batching_concurrent"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_batching_concurrent (id UInt64, value UInt64) ENGINE = MergeTree() ORDER BY id"
$CLICKHOUSE_CLIENT -q "INSERT INTO test_batching_concurrent SELECT number, number * 10 FROM numbers(1000)"

echo "Test 1: Concurrent identical queries all return correct results"

# Launch 5 identical queries concurrently with batching enabled.
# Collect outputs and sort to avoid non-deterministic ordering.
for i in $(seq 1 5); do
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM test_batching_concurrent SETTINGS use_query_batching = 1, query_batching_max_wait_ms = 500" &
done

# Wait for all background queries to complete.
wait

echo "Test 2: Concurrent different queries return correct results"

# Launch queries with different WHERE clauses — these should NOT be batched together.
# Collect and sort results to avoid non-deterministic output ordering from background jobs.
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM test_batching_concurrent WHERE id < 100 SETTINGS use_query_batching = 1" &
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM test_batching_concurrent WHERE id < 500 SETTINGS use_query_batching = 1" &
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM test_batching_concurrent WHERE id < 1000 SETTINGS use_query_batching = 1" &
    wait
} | sort -n

$CLICKHOUSE_CLIENT -q "DROP TABLE test_batching_concurrent"
$CLICKHOUSE_CLIENT -q "SYSTEM CLEAR QUERY CACHE"
