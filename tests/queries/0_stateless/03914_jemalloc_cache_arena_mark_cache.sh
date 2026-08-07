#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, use_jemalloc

# Test that mark cache allocations use the dedicated jemalloc cache arena.
# Arena pactive reclamation is tested by the integration test
# test_userspace_page_cache::test_cache_arena_isolation in an isolated environment.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_cache_arena_marks"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_cache_arena_marks (a UInt64, b String, c Float64)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0, prewarm_mark_cache = 0"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_cache_arena_marks
    SELECT number, toString(number), number * 1.1 FROM numbers(5000000)"

# Drop caches and record baseline
before_bytes=$($CLICKHOUSE_CLIENT -q "
    SYSTEM DROP MARK CACHE;
    SYSTEM DROP INDEX MARK CACHE;
    SYSTEM DROP UNCOMPRESSED CACHE;
    SYSTEM DROP INDEX UNCOMPRESSED CACHE;
    SYSTEM DROP PAGE CACHE;
    SELECT value FROM system.metrics WHERE metric = 'MarkCacheBytes'")
echo "before_select	0"

# Force mark loading
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM t_cache_arena_marks WHERE NOT ignore(*) FORMAT Null"

# Verify marks are cached (bytes should have increased)
after_bytes=$($CLICKHOUSE_CLIENT -q "
    SELECT value FROM system.metrics WHERE metric = 'MarkCacheBytes'")
echo "after_select	$(( after_bytes > before_bytes ? 1 : 0 ))"

# Verify cache arena is active (marks landed in the dedicated arena)
$CLICKHOUSE_CLIENT -q "
    SYSTEM RELOAD ASYNCHRONOUS METRICS;
    SELECT 'arena_active', value > 0 FROM system.asynchronous_metrics
    WHERE metric = 'jemalloc.cache_arena.pactive'"

# Drop the table so its background merges can't reload marks,
# then atomically clear caches and read MarkCacheBytes.
$CLICKHOUSE_CLIENT -q "DROP TABLE t_cache_arena_marks"

cache_cleared=0
for _ in $(seq 1 5); do
    cleared_bytes=$($CLICKHOUSE_CLIENT -q "
        SYSTEM DROP MARK CACHE;
        SYSTEM DROP INDEX MARK CACHE;
        SYSTEM DROP UNCOMPRESSED CACHE;
        SYSTEM DROP INDEX UNCOMPRESSED CACHE;
        SYSTEM DROP PAGE CACHE;
        SELECT value FROM system.metrics WHERE metric = 'MarkCacheBytes'")

    if [ "$cleared_bytes" -lt "$after_bytes" ]; then
        cache_cleared=1
        break
    fi
done

echo "after_clear	${cache_cleared}"
