#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
# The columns cache keeps no tombstone for a removed part: its entries are dropped by
# `IMergeTreeDataPart::clearCaches`, which runs when the part is finally gone. This test proves
# that the entries really do go away - both for the parts a merge replaces and for a dropped
# table - instead of sitting in the cache forever under a name nothing refers to any more.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Wait until no cache entry mentions any of the given parts of the table.
# The removal is done by the background cleanup thread, which the table settings below
# make run about once a second.
wait_for_parts_to_leave_the_cache()
{
    local uuid=$1
    local parts=$2

    for _ in {1..300}
    do
        local remaining
        remaining=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.columns_cache
        WHERE table_uuid = '$uuid' AND has($parts, part)
        ")

        if [ "$remaining" = "0" ]
        then
            echo "1"
            return
        fi

        sleep 0.3
    done

    echo "still cached after the timeout: $remaining"
}

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_columns_cache_removal"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_columns_cache_removal (id UInt64, value UInt64)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0,
         index_granularity = 1000,
         -- Remove outdated parts as soon as the merge is committed, and keep the cleanup
         -- thread on a fixed one-second schedule instead of letting it back off.
         old_parts_lifetime = 0,
         cleanup_delay_period = 1,
         cleanup_delay_period_random_add = 0,
         cleanup_thread_preferred_points_per_iteration = 0
"

# The part names are asserted below, so no background merge may rearrange them
# before `OPTIMIZE` does it explicitly.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_columns_cache_removal"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_columns_cache_removal SELECT number, number * 2 FROM numbers(2000)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_columns_cache_removal SELECT number + 2000, (number + 2000) * 2 FROM numbers(2000)"

# Cache entries are keyed by the table UUID, and nothing can resolve it back to a name
# once the table is gone, so remember it while the table still exists.
table_uuid=$($CLICKHOUSE_CLIENT -q "
SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_columns_cache_removal'
")

$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

$CLICKHOUSE_CLIENT -q "
SELECT sum(id), sum(value), count() FROM t_columns_cache_removal
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, max_threads = 1
"

echo -n 'cached source parts before the merge: '
$CLICKHOUSE_CLIENT -q "
SELECT arraySort(groupUniqArray(part)) FROM system.columns_cache WHERE table_uuid = '$table_uuid'
"

$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_columns_cache_removal"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_columns_cache_removal FINAL"

echo -n 'source part entries removed after the merge: '
wait_for_parts_to_leave_the_cache "$table_uuid" "['all_1_1_0', 'all_2_2_0']"

# The merged part is a part of its own: it has to miss once and is served from the cache
# afterwards. This is what would silently break if the entries of the source parts were
# reused for it.
$CLICKHOUSE_CLIENT -q "
SELECT sum(id), sum(value), count() FROM t_columns_cache_removal
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1,
         max_threads = 1, log_queries = 1, log_comment = '05136_merged_part_read_1'
"

$CLICKHOUSE_CLIENT -q "
SELECT sum(id), sum(value), count() FROM t_columns_cache_removal
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1,
         max_threads = 1, log_queries = 1, log_comment = '05136_merged_part_read_2'
"

echo -n 'only the merged part is cached: '
$CLICKHOUSE_CLIENT -q "
SELECT arraySort(groupUniqArray(part)) FROM system.columns_cache WHERE table_uuid = '$table_uuid'
"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT -q "
SELECT
    log_comment,
    ProfileEvents['ColumnsCacheHits'] > 0 AS has_hits,
    ProfileEvents['ColumnsCacheMisses'] > 0 AS has_misses
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment LIKE '05136_merged_part_read_%'
ORDER BY log_comment
"

# Dropping the table purges its entries at once - `MergeTreeData::dropAllData` invalidates the
# whole table - so the count is exact and needs no waiting.
$CLICKHOUSE_CLIENT -q "DROP TABLE t_columns_cache_removal SYNC"

echo -n 'entries left after the drop: '
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.columns_cache WHERE table_uuid = '$table_uuid'"
