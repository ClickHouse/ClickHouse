#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, use_jemalloc

# Test that mark cache allocations use the dedicated jemalloc cache arena
# and that SYSTEM DROP MARK CACHE properly reclaims arena pages.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function drop_caches_and_get_pactive()
{
    $CLICKHOUSE_CLIENT -q "
        SYSTEM DROP MARK CACHE;
        SYSTEM DROP INDEX MARK CACHE;
        SYSTEM DROP UNCOMPRESSED CACHE;
        SYSTEM DROP INDEX UNCOMPRESSED CACHE;
        SYSTEM DROP PAGE CACHE;
        SYSTEM RELOAD ASYNCHRONOUS METRICS;
        SELECT value FROM system.asynchronous_metrics
        WHERE metric = 'jemalloc.cache_arena.pactive'"
}

function drop_caches_and_get_mark_cache_bytes()
{
    $CLICKHOUSE_CLIENT -q "
        SYSTEM DROP MARK CACHE;
        SYSTEM DROP INDEX MARK CACHE;
        SYSTEM DROP UNCOMPRESSED CACHE;
        SYSTEM DROP INDEX UNCOMPRESSED CACHE;
        SYSTEM DROP PAGE CACHE;
        SELECT value FROM system.metrics WHERE metric = 'MarkCacheBytes'"
}

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_cache_arena_marks"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_cache_arena_marks (a UInt64, b String, c Float64)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0, prewarm_mark_cache = 0"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_cache_arena_marks
    SELECT number, toString(number), number * 1.1 FROM numbers(5000000)"

# Record baseline (may be non-zero due to other tables)
before_bytes=$(drop_caches_and_get_mark_cache_bytes)
echo "before_select	0"

# Force mark loading
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM t_cache_arena_marks WHERE NOT ignore(*) FORMAT Null"

# Verify marks are cached (bytes should have increased)
after_bytes=$($CLICKHOUSE_CLIENT -q "
    SELECT value FROM system.metrics WHERE metric = 'MarkCacheBytes'")
echo "after_select	$(( after_bytes > before_bytes ? 1 : 0 ))"

# Read pactive after loading marks
$CLICKHOUSE_CLIENT -q "SYSTEM RELOAD ASYNCHRONOUS METRICS"
pactive_loaded=$($CLICKHOUSE_CLIENT -q "
    SELECT value FROM system.asynchronous_metrics
    WHERE metric = 'jemalloc.cache_arena.pactive'")

echo "arena_active	$(( pactive_loaded > 0 ? 1 : 0 ))"

# Drop our table so its background merges can't reload marks.
$CLICKHOUSE_CLIENT -q "DROP TABLE t_cache_arena_marks"

# Retry loop: atomically clear caches and read metrics.
# Other tables' background activity can reload marks between iterations,
# so we retry a few times.
reclaimed=0
cache_cleared=0
for _ in $(seq 1 5); do
    pactive_cleared=$(drop_caches_and_get_pactive)
    cleared_bytes=$(drop_caches_and_get_mark_cache_bytes)

    if [ "$pactive_cleared" -lt "$pactive_loaded" ]; then
        reclaimed=1
    fi
    if [ "$cleared_bytes" -lt "$after_bytes" ]; then
        cache_cleared=1
    fi
    if [ "$reclaimed" -eq 1 ] && [ "$cache_cleared" -eq 1 ]; then
        break
    fi
done

echo "after_clear	${cache_cleared}"
echo "arena_reclaimed	${reclaimed}"
