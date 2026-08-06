#!/usr/bin/env bash
# Tags: no-shared-merge-tree, memory-engine

# The per-table cache of shared part column descriptions: entries appear as parts with a new
# schema are added and disappear once the last part holding a column list is gone.
#
# DETACH PART is asynchronous in two ways (the detached part is replaced by a new empty covering
# part and the replaced part object is removed from memory by background cleanup, and a
# detach+attach of the table can even reload the not-yet-removed directory as a covered part), so
# the assertions after it poll until the expected value with a short parts lifetime. The
# assertions established synchronously by the preceding statement stay exact reads.
# Merges are disabled because the parts below are detached by name.
# Tagged no-shared-merge-tree: the eviction under test is driven by the lifecycle of local part
# objects, which SharedMergeTree manages through Keeper on its own schedule, so the polls do not
# converge there within the test time budget.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cache_size()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT columns_descriptions_cache_size FROM system.tables WHERE database = currentDatabase() AND table = '$1'"
}

# Prints the cache size of table $1 once it reaches $2 (or the last observed value on timeout,
# so a failure shows the actual state).
function wait_for_cache_size()
{
    local res
    for _ in {1..200}
    do
        res=$(cache_size "$1")
        [ "$res" = "$2" ] && break
        sleep 0.3
    done
    echo "$res"
}

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_mem"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_mt"

echo "-- the cache is only for MergeTree"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_mem (key Int) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_mem VALUES (1)"
cache_size t_mem

echo "-- MergeTree: an entry per distinct column list of the alive parts"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_mt (key Int) ENGINE = MergeTree ORDER BY () SETTINGS old_parts_lifetime = 1, max_bytes_to_merge_at_max_space_in_pool = 1"
cache_size t_mt
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_mt VALUES (1)"
cache_size t_mt
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_mt VALUES (2)"
cache_size t_mt
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_mt ADD COLUMN value String SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_mt VALUES (10, '10')"
cache_size t_mt
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_mt VALUES (20, '20')"
cache_size t_mt

echo "-- the entry of the old structure must be evicted once both its parts are gone"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_mt DETACH PART 'all_1_1_0'"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_mt DETACH PART 'all_2_2_0'"
wait_for_cache_size t_mt 1

echo "-- and after a detach/attach round trip of the table"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_mt"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_mt"
wait_for_cache_size t_mt 1

echo "-- system.metrics"
${CLICKHOUSE_CLIENT} --query "SELECT value > 0 FROM system.metrics WHERE metric = 'ColumnsDescriptionsCacheSize'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_mem"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_mt"
