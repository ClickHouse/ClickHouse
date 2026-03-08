#!/usr/bin/env bash
# Tags: no-fasttest, long, no-parallel, no-msan, no-random-settings, no-random-merge-tree-settings, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

# Concurrent test for columns cache
# Tests various scenarios with multiple threads accessing the cache simultaneously
# NOTE: Avoids Nullable columns with WHERE clauses due to known PREWHERE bug

echo "Setting up test tables..."

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_cache_concurrent_1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_cache_concurrent_2"

# Table 1: Simple numeric and String types (no Nullable to avoid PREWHERE bug)
$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_cache_concurrent_1 (
    id UInt64,
    value UInt64,
    str String
) ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;
"

# Table 2: More complex structure
$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_cache_concurrent_2 (
    pk UInt64,
    category UInt32,
    data String
) ENGINE = MergeTree
ORDER BY pk
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;
"

echo "Inserting data..."

# Insert substantial data to have multiple granules
$CLICKHOUSE_CLIENT -q "
INSERT INTO t_cache_concurrent_1
SELECT
    number AS id,
    number * 2 AS value,
    'string_' || toString(number) AS str
FROM numbers(10000);
"

$CLICKHOUSE_CLIENT -q "
INSERT INTO t_cache_concurrent_2
SELECT
    number AS pk,
    number % 100 AS category,
    'data_' || toString(number) AS data
FROM numbers(10000);
"

# Drop and enable cache
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT -q "SET use_columns_cache = 1"

echo "Running concurrent queries..."

# Function to run query multiple times
run_queries() {
    local query="$1"
    local count="$2"
    for i in $(seq 1 "$count"); do
        $CLICKHOUSE_CLIENT -q "$query" > /dev/null
    done
}

# Scenario 1: Multiple readers of same data
echo "Scenario 1: Multiple readers of same data..."
for i in {1..10}; do
    run_queries "SELECT sum(id), sum(value) FROM t_cache_concurrent_1 SETTINGS use_columns_cache = 1" 2 &
done
wait

# Scenario 2: Readers of different columns
echo "Scenario 2: Readers of different columns..."
for i in {1..5}; do
    run_queries "SELECT sum(id) FROM t_cache_concurrent_1 SETTINGS use_columns_cache = 1" 2 &
    run_queries "SELECT sum(value) FROM t_cache_concurrent_1 SETTINGS use_columns_cache = 1" 2 &
    run_queries "SELECT count(str) FROM t_cache_concurrent_1 SETTINGS use_columns_cache = 1" 2 &
done
wait

# Scenario 3: String columns and subcolumns
echo "Scenario 3: String columns and subcolumns..."
for i in {1..5}; do
    run_queries "SELECT sum(length(str)) FROM t_cache_concurrent_1 SETTINGS use_columns_cache = 1" 2 &
    run_queries "SELECT any(str) FROM t_cache_concurrent_1 SETTINGS use_columns_cache = 1" 2 &
done
wait

# Scenario 4: Overlapping range reads
echo "Scenario 4: Overlapping range reads..."
for i in {1..5}; do
    run_queries "SELECT sum(id) FROM t_cache_concurrent_1 WHERE id < 5000 SETTINGS use_columns_cache = 1" 2 &
    run_queries "SELECT sum(id) FROM t_cache_concurrent_1 WHERE id >= 3000 AND id < 8000 SETTINGS use_columns_cache = 1" 2 &
    run_queries "SELECT sum(id) FROM t_cache_concurrent_1 WHERE id >= 5000 SETTINGS use_columns_cache = 1" 2 &
done
wait

# Scenario 5: Mix with aggregations
echo "Scenario 5: Mix with aggregations..."
for i in {1..5}; do
    run_queries "SELECT category, sum(pk) FROM t_cache_concurrent_2 GROUP BY category LIMIT 10 SETTINGS use_columns_cache = 1" 2 &
    run_queries "SELECT count(DISTINCT category) FROM t_cache_concurrent_2 SETTINGS use_columns_cache = 1" 2 &
done
wait

# Scenario 6: Cache eviction under load
echo "Scenario 6: Cache eviction under load..."
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
for i in {1..10}; do
    run_queries "SELECT sum(id), sum(value), count(str) FROM t_cache_concurrent_1 SETTINGS use_columns_cache = 1" 3 &
    run_queries "SELECT sum(pk), count(data) FROM t_cache_concurrent_2 SETTINGS use_columns_cache = 1" 3 &
done
wait

# Scenario 7: Concurrent reads and cache drops
echo "Scenario 7: Concurrent reads with cache drops..."
for i in {1..3}; do
    run_queries "SELECT sum(id) FROM t_cache_concurrent_1 SETTINGS use_columns_cache = 1" 5 &
    sleep 0.1
    $CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE" > /dev/null 2>&1 || true &
done
wait

# Scenario 8: Heavy concurrent load
echo "Scenario 8: Heavy concurrent load..."
for i in {1..10}; do
    run_queries "SELECT sum(id), avg(value) FROM t_cache_concurrent_1 SETTINGS use_columns_cache = 1" 1 &
    run_queries "SELECT sum(pk), count(category) FROM t_cache_concurrent_2 SETTINGS use_columns_cache = 1" 1 &
done
wait

# Verify data correctness
echo "Verifying data correctness..."

result1=$($CLICKHOUSE_CLIENT -q "SELECT sum(id), sum(value) FROM t_cache_concurrent_1 SETTINGS use_columns_cache = 1")
expected1="49995000	99990000"
if [ "$result1" != "$expected1" ]; then
    echo "ERROR: Table 1 verification failed. Expected: $expected1, Got: $result1"
    exit 1
fi

result2=$($CLICKHOUSE_CLIENT -q "SELECT sum(pk), count() FROM t_cache_concurrent_2 SETTINGS use_columns_cache = 1")
expected2="49995000	10000"
if [ "$result2" != "$expected2" ]; then
    echo "ERROR: Table 2 verification failed. Expected: $expected2, Got: $result2"
    exit 1
fi

# Test String subcolumn caching under concurrent load
echo "Testing String subcolumn concurrent access..."
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

for i in {1..10}; do
    run_queries "SELECT sum(length(str)) FROM t_cache_concurrent_1 SETTINGS use_columns_cache = 1" 2 &
    run_queries "SELECT any(substr(str, 1, 5)) FROM t_cache_concurrent_1 SETTINGS use_columns_cache = 1" 2 &
done
wait

# Verify String results
result_str=$($CLICKHOUSE_CLIENT -q "SELECT sum(length(str)) FROM t_cache_concurrent_1 SETTINGS use_columns_cache = 1")
if [ -z "$result_str" ]; then
    echo "ERROR: String subcolumn test failed"
    exit 1
fi

# Cleanup
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_cache_concurrent_1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_cache_concurrent_2"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

echo "All concurrent tests passed!"
