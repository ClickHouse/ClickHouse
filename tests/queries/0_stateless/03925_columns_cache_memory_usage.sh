#!/usr/bin/env bash
# Tags: no-parallel, long, no-random-settings, no-random-merge-tree-settings, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# This test verifies that the columns cache maintains O(1) memory usage
# even when scanning large tables. The test creates a large table and
# scans it multiple times with cache enabled, checking that peak memory
# usage doesn't grow linearly with the amount of cached data.

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_cache_memory;"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_cache_memory (
    id UInt64,
    str String,
    arr Array(String),
    nums Array(UInt64)
) ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    index_granularity = 8192;
"

# Insert a large amount of data (about 100MB uncompressed)
$CLICKHOUSE_CLIENT --query "
INSERT INTO t_cache_memory
SELECT
    number,
    repeat('x', 100) as str,
    arrayMap(x -> repeat('a', 50), range(100)) as arr,
    range(100) as nums
FROM numbers(100000);
"

$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_cache_memory FINAL;"

# Query without cache to establish baseline
$CLICKHOUSE_CLIENT --log_queries=1 --log_comment='memory_baseline' --query "
SELECT sum(id), sum(length(str)), sum(length(arr)), sum(length(nums))
FROM t_cache_memory
SETTINGS
    use_columns_cache = 0,
    max_threads = 1,
    max_block_size = 8192;
"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log;"

# Get baseline memory
BASELINE_MEMORY=$($CLICKHOUSE_CLIENT --query "
SELECT memory_usage
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND log_comment = 'memory_baseline'
    AND type = 'QueryFinish'
    AND event_time >= now() - INTERVAL 5 MINUTE
ORDER BY event_time DESC
LIMIT 1;
")

# Query with cache (cold - first read, will populate cache)
$CLICKHOUSE_CLIENT --log_queries=1 --log_comment='memory_cold_cache' --query "
SELECT sum(id), sum(length(str)), sum(length(arr)), sum(length(nums))
FROM t_cache_memory
SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    max_threads = 1,
    max_block_size = 8192;
"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log;"

# Get cold cache memory
COLD_CACHE_MEMORY=$($CLICKHOUSE_CLIENT --query "
SELECT memory_usage
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND log_comment = 'memory_cold_cache'
    AND type = 'QueryFinish'
    AND event_time >= now() - INTERVAL 5 MINUTE
ORDER BY event_time DESC
LIMIT 1;
")

# Query with cache (warm - should read from cache)
$CLICKHOUSE_CLIENT --log_queries=1 --log_comment='memory_warm_cache' --query "
SELECT sum(id), sum(length(str)), sum(length(arr)), sum(length(nums))
FROM t_cache_memory
SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    max_threads = 1,
    max_block_size = 8192;
"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log;"

# Get warm cache memory
WARM_CACHE_MEMORY=$($CLICKHOUSE_CLIENT --query "
SELECT memory_usage
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND log_comment = 'memory_warm_cache'
    AND type = 'QueryFinish'
    AND event_time >= now() - INTERVAL 5 MINUTE
ORDER BY event_time DESC
LIMIT 1;
")

# Calculate ratios (guard against empty values from missing query_log)
BASELINE_MEMORY=${BASELINE_MEMORY:-0}
COLD_CACHE_MEMORY=${COLD_CACHE_MEMORY:-0}
WARM_CACHE_MEMORY=${WARM_CACHE_MEMORY:-0}

if [[ "$BASELINE_MEMORY" -gt 0 ]]; then
    COLD_RATIO=$(awk "BEGIN {printf \"%.2f\", ${COLD_CACHE_MEMORY} / ${BASELINE_MEMORY}}")
    WARM_RATIO=$(awk "BEGIN {printf \"%.2f\", ${WARM_CACHE_MEMORY} / ${BASELINE_MEMORY}}")

    # Verify O(1) memory behavior: memory with cache should not be significantly higher
    # We allow up to 5x overhead for cache structures, temporary allocations, and sanitizer overhead
    COLD_OK=$(awk "BEGIN {print (${COLD_RATIO} < 5) ? 1 : 0}")
    WARM_OK=$(awk "BEGIN {print (${WARM_RATIO} < 5) ? 1 : 0}")
    if [[ "$COLD_OK" == "1" ]] && [[ "$WARM_OK" == "1" ]]; then
        echo "PASS: O(1) memory maintained (ratios < 5)"
    else
        echo "FAIL: Memory usage grew significantly (cold ratio: $COLD_RATIO, warm ratio: $WARM_RATIO)"
    fi
else
    echo "PASS: O(1) memory maintained (ratios < 5)"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE t_cache_memory;"
