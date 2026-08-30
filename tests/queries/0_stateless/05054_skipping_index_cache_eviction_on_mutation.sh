#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings
# no-parallel: checks server-wide SkippingIndexCacheCells metric
# no-random-settings: old_parts_lifetime = 0 must not be overridden

# Entries of the skipping index cache must be evicted when old parts are removed after a mutation.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS tab"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP SKIPPING INDEX CACHE"

$CLICKHOUSE_CLIENT --query "SELECT metric, value FROM system.metrics WHERE metric = 'SkippingIndexCacheCells'"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE tab (id UInt64, key UInt64, INDEX idx_key key TYPE bloom_filter(0.01) GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 16, old_parts_lifetime = 0, merge_tree_clear_old_parts_interval_seconds = 1, cleanup_delay_period = 0, cleanup_delay_period_random_add = 0, cleanup_thread_preferred_points_per_iteration = 0
"

$CLICKHOUSE_CLIENT --query "INSERT INTO tab SELECT number, number % 100 FROM numbers(4800)"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM tab WHERE key = 42"

cells_before=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.metrics WHERE metric = 'SkippingIndexCacheCells'")
$CLICKHOUSE_CLIENT --query "SELECT 'SkippingIndexCacheCells', if($cells_before > 0, 'Populated', 'UNEXPECTED_ZERO')"

# The mutation replaces the part; the old part becomes Outdated and is removed soon after.
$CLICKHOUSE_CLIENT --query "ALTER TABLE tab DELETE WHERE id = 0 SETTINGS mutations_sync = 2"

# Poll the metric, because the eviction happens when the old part is removed from the filesystem,
# which is later than the part disappears from system.parts.
cells_decreased=0
for _ in $(seq 1 60); do
    cells_after=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.metrics WHERE metric = 'SkippingIndexCacheCells'")
    if [ "$cells_after" -lt "$cells_before" ]; then
        cells_decreased=1
        break
    fi
    sleep 1
done

if [ "$cells_decreased" -eq 1 ]; then
    echo "SkippingIndexCacheCells	Decreased"
else
    echo "SkippingIndexCacheCells	NOT_DECREASED (before=$cells_before, after=$cells_after)"
fi

# The result is still correct with the new part.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM tab WHERE key = 42"

$CLICKHOUSE_CLIENT --query "DROP TABLE tab SYNC"
