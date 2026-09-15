#!/usr/bin/env bash
# Tags: no-parallel, long, no-random-settings, no-random-merge-tree-settings, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# This test verifies two distinct memory properties of the columns cache.
#
# Part 1 measures the per-query memory overhead of the cache: the peak memory of one
# scan of a large table with the cache enabled (cold and warm) is compared against the
# same scan with the cache disabled. This says nothing about the lifetime memory of the
# cache itself - it only bounds what a single query pays for using it.
#
# Part 2 measures the advertised property that a query's memory does not grow with the
# amount of data retained in the cache: the same fixed scan is measured once while only
# its own table is cached, and again after several more tables have been warmed, so that
# the cache holds several times more data. The test also asserts that the cache really
# did grow in between (otherwise the comparison would be vacuous) and that the data it
# retains stays within the configured `columns_cache_size`.
#
# Both parts assert that the cache was really engaged (`ColumnsCacheHits` is checked), so
# that neither of them can silently compare the cache-disabled path against itself.
#
# Part 1 raises the block limits so that the whole read task is one block. The cache stores
# one entry per mark range of a task, so a cache-populating query holds a copy of the rows
# of a range until the range has been read to its end, however small its blocks are; with
# the default block limits the baseline query would hold one block of the range at a time
# instead, and the comparison would measure the block limits rather than the cache. With the
# limits raised, both queries hold one range at a time.

# Settings that let a whole read task be read in one block.
BLOCK_SETTINGS="max_threads = 1, max_block_size = 200000, preferred_block_size_bytes = 0"

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

$CLICKHOUSE_CLIENT --query "SYSTEM DROP COLUMNS CACHE;"

# Query without cache to establish baseline
$CLICKHOUSE_CLIENT --log_queries=1 --log_comment='memory_baseline' --query "
SELECT sum(id), sum(length(str)), sum(length(arr)), sum(length(nums))
FROM t_cache_memory
SETTINGS use_columns_cache = 0, $BLOCK_SETTINGS;
"

# Query with cache (cold - first read, will populate cache)
$CLICKHOUSE_CLIENT --log_queries=1 --log_comment='memory_cold_cache' --query "
SELECT sum(id), sum(length(str)), sum(length(arr)), sum(length(nums))
FROM t_cache_memory
SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    $BLOCK_SETTINGS;
"

# Query with cache (warm - should read from cache)
$CLICKHOUSE_CLIENT --log_queries=1 --log_comment='memory_warm_cache' --query "
SELECT sum(id), sum(length(str)), sum(length(arr)), sum(length(nums))
FROM t_cache_memory
SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    $BLOCK_SETTINGS;
"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log;"

read_metric()
{
    $CLICKHOUSE_CLIENT --query "
    SELECT $2
    FROM system.query_log
    WHERE
        current_database = currentDatabase()
        AND log_comment = '$1'
        AND type = 'QueryFinish'
        AND event_time >= now() - INTERVAL 5 MINUTE
    ORDER BY event_time DESC
    LIMIT 1;
    "
}

BASELINE_MEMORY=$(read_metric 'memory_baseline' 'memory_usage')
COLD_CACHE_MEMORY=$(read_metric 'memory_cold_cache' 'memory_usage')
WARM_CACHE_MEMORY=$(read_metric 'memory_warm_cache' 'memory_usage')

# The warm query must really have been served from the cache, otherwise the ratios
# below compare the cache-disabled path against itself and prove nothing.
WARM_HITS=$(read_metric 'memory_warm_cache' "ProfileEvents['ColumnsCacheHits']")

# Fail closed: all three measurements must exist and be positive. Without the
# evidence there is nothing to compare, so the test must not claim success.
if ! [[ "$BASELINE_MEMORY" =~ ^[0-9]+$ ]] || [[ "$BASELINE_MEMORY" -le 0 ]] \
    || ! [[ "$COLD_CACHE_MEMORY" =~ ^[0-9]+$ ]] || [[ "$COLD_CACHE_MEMORY" -le 0 ]] \
    || ! [[ "$WARM_CACHE_MEMORY" =~ ^[0-9]+$ ]] || [[ "$WARM_CACHE_MEMORY" -le 0 ]]
then
    echo "FAIL: missing memory measurements (baseline: '$BASELINE_MEMORY', cold: '$COLD_CACHE_MEMORY', warm: '$WARM_CACHE_MEMORY')"
elif ! [[ "$WARM_HITS" =~ ^[0-9]+$ ]] || [[ "$WARM_HITS" -le 0 ]]
then
    echo "FAIL: the warm query did not read from the cache (hits: '$WARM_HITS')"
else
    COLD_RATIO=$(awk "BEGIN {printf \"%.2f\", ${COLD_CACHE_MEMORY} / ${BASELINE_MEMORY}}")
    WARM_RATIO=$(awk "BEGIN {printf \"%.2f\", ${WARM_CACHE_MEMORY} / ${BASELINE_MEMORY}}")

    # A single query should not pay much for the cache: we allow up to 5x over the
    # cache-less baseline for cache structures, temporary allocations and sanitizer overhead.
    COLD_OK=$(awk "BEGIN {print (${COLD_RATIO} < 5) ? 1 : 0}")
    WARM_OK=$(awk "BEGIN {print (${WARM_RATIO} < 5) ? 1 : 0}")
    if [[ "$COLD_OK" == "1" ]] && [[ "$WARM_OK" == "1" ]]; then
        echo "PASS: per-query memory overhead is bounded (ratios < 5)"
    else
        echo "FAIL: per-query memory overhead grew significantly (cold ratio: $COLD_RATIO, warm ratio: $WARM_RATIO)"
    fi
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE t_cache_memory;"

# =============================================================================
# Part 2: the memory of a fixed query must not grow with the amount of data
# retained in the cache.
# =============================================================================

# Six tables of identical shape. Everything together stays well below the configured
# cache size, so warming more of them really does grow the retained data instead of
# evicting the table that is being measured.
for i in 0 1 2 3 4 5
do
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_cache_growth_$i;"
    $CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_cache_growth_$i (id UInt64, str String)
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS
        min_bytes_for_wide_part = 0,
        index_granularity = 8192;
    "
    $CLICKHOUSE_CLIENT --query "
    INSERT INTO t_cache_growth_$i SELECT number, repeat('y', 200) FROM numbers(40000);
    "
done

$CLICKHOUSE_CLIENT --query "SYSTEM DROP COLUMNS CACHE;"

# Part 2 keeps the default block limits: every task is read in several blocks here, so the
# cache is populated and served across continuation reads, which is the ordinary case.
CACHE_SETTINGS="use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, max_threads = 1"

# Warm the measured table, then measure a warm scan of it while it is the only thing
# in the cache.
$CLICKHOUSE_CLIENT --query "
SELECT sum(id), sum(length(str)) FROM t_cache_growth_0 SETTINGS $CACHE_SETTINGS FORMAT Null;
"

$CLICKHOUSE_CLIENT --log_queries=1 --log_comment='memory_growth_small_cache' --query "
SELECT sum(id), sum(length(str)) FROM t_cache_growth_0 SETTINGS $CACHE_SETTINGS FORMAT Null;
"

SMALL_CACHE_BYTES=$($CLICKHOUSE_CLIENT --query "SELECT sum(bytes) FROM system.columns_cache;")

# Warm the five other tables, so the cache retains several times more data.
for i in 1 2 3 4 5
do
    $CLICKHOUSE_CLIENT --query "
    SELECT sum(id), sum(length(str)) FROM t_cache_growth_$i SETTINGS $CACHE_SETTINGS FORMAT Null;
    "
done

LARGE_CACHE_BYTES=$($CLICKHOUSE_CLIENT --query "SELECT sum(bytes) FROM system.columns_cache;")

# The very same query again, now against a much larger cache.
$CLICKHOUSE_CLIENT --log_queries=1 --log_comment='memory_growth_large_cache' --query "
SELECT sum(id), sum(length(str)) FROM t_cache_growth_0 SETTINGS $CACHE_SETTINGS FORMAT Null;
"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log;"

SMALL_CACHE_MEMORY=$(read_metric 'memory_growth_small_cache' 'memory_usage')
LARGE_CACHE_MEMORY=$(read_metric 'memory_growth_large_cache' 'memory_usage')
# Both measured scans must have been served from the cache. If the first one had missed, the
# ratio below would compare a read from disk against a read from the cache, and would say
# nothing about the memory of a query being independent of the data retained in the cache.
SMALL_CACHE_HITS=$(read_metric 'memory_growth_small_cache' "ProfileEvents['ColumnsCacheHits']")
LARGE_CACHE_HITS=$(read_metric 'memory_growth_large_cache' "ProfileEvents['ColumnsCacheHits']")

# The retained data must never exceed the configured cache size - that is what makes
# the cache safe to leave enabled.
CACHE_SIZE_RESPECTED=$($CLICKHOUSE_CLIENT --query "
SELECT (SELECT sum(bytes) FROM system.columns_cache) <= (SELECT value::UInt64 FROM system.server_settings WHERE name = 'columns_cache_size');
")

# Fail closed again: without all the measurements, and without proof that the cache
# actually grew and was actually used, the comparison below would mean nothing.
if ! [[ "$SMALL_CACHE_MEMORY" =~ ^[0-9]+$ ]] || [[ "$SMALL_CACHE_MEMORY" -le 0 ]] \
    || ! [[ "$LARGE_CACHE_MEMORY" =~ ^[0-9]+$ ]] || [[ "$LARGE_CACHE_MEMORY" -le 0 ]] \
    || ! [[ "$SMALL_CACHE_BYTES" =~ ^[0-9]+$ ]] || [[ "$SMALL_CACHE_BYTES" -le 0 ]] \
    || ! [[ "$LARGE_CACHE_BYTES" =~ ^[0-9]+$ ]]
then
    echo "FAIL: missing growth measurements (small: '$SMALL_CACHE_MEMORY'/'$SMALL_CACHE_BYTES', large: '$LARGE_CACHE_MEMORY'/'$LARGE_CACHE_BYTES')"
elif ! [[ "$SMALL_CACHE_HITS" =~ ^[0-9]+$ ]] || [[ "$SMALL_CACHE_HITS" -le 0 ]] \
    || ! [[ "$LARGE_CACHE_HITS" =~ ^[0-9]+$ ]] || [[ "$LARGE_CACHE_HITS" -le 0 ]]
then
    echo "FAIL: a measured query did not read from the cache (hits: '$SMALL_CACHE_HITS' with the small cache, '$LARGE_CACHE_HITS' with the large one)"
elif [[ "$CACHE_SIZE_RESPECTED" != "1" ]]
then
    echo "FAIL: retained cache data exceeds columns_cache_size"
else
    CACHE_GROWTH=$(awk "BEGIN {printf \"%.2f\", ${LARGE_CACHE_BYTES} / ${SMALL_CACHE_BYTES}}")
    GREW=$(awk "BEGIN {print (${CACHE_GROWTH} >= 4) ? 1 : 0}")
    if [[ "$GREW" != "1" ]]
    then
        echo "FAIL: the cache did not grow enough to make the comparison meaningful (growth: $CACHE_GROWTH)"
    else
        MEMORY_RATIO=$(awk "BEGIN {printf \"%.2f\", ${LARGE_CACHE_MEMORY} / ${SMALL_CACHE_MEMORY}}")
        # Linear growth would show up as roughly the same factor as the cache grew by
        # (at least 4x here); a constant per-query cost stays close to 1.
        RATIO_OK=$(awk "BEGIN {print (${MEMORY_RATIO} < 3) ? 1 : 0}")
        if [[ "$RATIO_OK" == "1" ]]; then
            echo "PASS: query memory did not grow with the cached data (ratio < 3)"
        else
            echo "FAIL: query memory grew with the cached data (cache growth: $CACHE_GROWTH, memory ratio: $MEMORY_RATIO)"
        fi
    fi
fi

for i in 0 1 2 3 4 5
do
    $CLICKHOUSE_CLIENT --query "DROP TABLE t_cache_growth_$i;"
done
