#!/usr/bin/env bash
# Tags: no-shared-merge-tree

# Eviction of the shared part metadata cache across ALTER ADD/DROP COLUMN cycles,
# with and without Nested columns and with `share_nested_offsets` enabled and disabled
# (the regression matrix of https://github.com/ClickHouse/ClickHouse/issues/102926).
#
# Parts replaced by mutations stay alive as Outdated part objects until the old parts cleanup
# runs, and each alive part object holds its cache entry. For the mutation counts below
# `old_parts_lifetime` is pinned high so they are deterministic. DETACH PART is asynchronous
# in two ways (the replaced part object is removed from memory and disk by background cleanup,
# and a detach+attach of the table can even reload the not-yet-removed directory as a covered
# part), so the assertions after it poll until the expected value with a short parts lifetime.
# Merges and the persistent `_block_number` / `_block_offset` columns are disabled: parts that store
# them have one more column list, which is one more cache entry.
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

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_evict_plain"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_evict_nested"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_evict_nested_noshare"

echo "-- plain table, ADD/DROP COLUMN cycles (counts include entries held by pinned Outdated parts)"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_evict_plain (key Int) ENGINE = MergeTree ORDER BY key SETTINGS old_parts_lifetime = 480, max_bytes_to_merge_at_max_space_in_pool = 1, enable_block_number_column = 0, enable_block_offset_column = 0"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_evict_plain VALUES (1)"
cache_size t_evict_plain
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_evict_plain ADD COLUMN v1 String SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_evict_plain VALUES (2, '2')"
cache_size t_evict_plain
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_evict_plain DROP COLUMN v1 SETTINGS mutations_sync = 2"
cache_size t_evict_plain
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_evict_plain ADD COLUMN v2 Nullable(Int64) SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_evict_plain VALUES (3, 3)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_evict_plain DROP COLUMN v2 SETTINGS mutations_sync = 2"
cache_size t_evict_plain
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(key) FROM t_evict_plain"

echo "-- Nested table: the entry of a detached part must be evicted once its object is gone"
echo "-- (this was the leak in issue 102926: entries for schemas with a distinct collected-nested description were never evicted)"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_evict_nested (key Int, \`n.a\` Array(Int32), \`n.b\` Array(String)) ENGINE = MergeTree ORDER BY key SETTINGS old_parts_lifetime = 1, max_bytes_to_merge_at_max_space_in_pool = 1, enable_block_number_column = 0, enable_block_offset_column = 0"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_evict_nested VALUES (1, [10], ['hello'])"
cache_size t_evict_nested
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_evict_nested ADD COLUMN value String SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_evict_nested VALUES (2, [20], ['world'], '2')"
cache_size t_evict_nested
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_evict_nested DETACH PART 'all_1_1_0'"
wait_for_cache_size t_evict_nested 1
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_evict_nested"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_evict_nested"
wait_for_cache_size t_evict_nested 1
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_evict_nested"

echo "-- the same with share_nested_offsets disabled"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_evict_nested_noshare (key Int, \`n.a\` Array(Int32), \`n.b\` Array(String)) ENGINE = MergeTree ORDER BY key SETTINGS share_nested_offsets = 0, old_parts_lifetime = 1, max_bytes_to_merge_at_max_space_in_pool = 1, enable_block_number_column = 0, enable_block_offset_column = 0"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_evict_nested_noshare VALUES (1, [10], ['hello'])"
cache_size t_evict_nested_noshare
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_evict_nested_noshare ADD COLUMN value String SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_evict_nested_noshare VALUES (2, [20], ['world'], '2')"
cache_size t_evict_nested_noshare
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_evict_nested_noshare DETACH PART 'all_1_1_0'"
wait_for_cache_size t_evict_nested_noshare 1
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_evict_nested_noshare"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_evict_plain"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_evict_nested"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_evict_nested_noshare"
