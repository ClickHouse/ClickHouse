#!/usr/bin/env bash
# Tags: no-shared-merge-tree

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/102926:
# `MergeTreeData::releaseSharedPartColumns` must evict cache entries for schemas with `Nested`
# columns, where `Nested::collect` produces a distinct `with_collected_nested` description.
#
# DETACH PART is asynchronous (the detached part is replaced by a new empty covering part and the
# replaced part object is removed from memory by background cleanup), so the assertion after it
# polls until the expected value with a short parts lifetime. The assertions established
# synchronously by the preceding statement stay exact reads.
# Merges are disabled because the part below is detached by name.
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

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_nested_leak"

echo "-- an entry per distinct column list of the alive parts"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_nested_leak (key Int, \`n.a\` Array(Int32), \`n.b\` Array(String)) ENGINE = MergeTree ORDER BY key SETTINGS old_parts_lifetime = 1, max_bytes_to_merge_at_max_space_in_pool = 1"
cache_size t_nested_leak
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_nested_leak VALUES (1, [10], ['hello'])"
cache_size t_nested_leak
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_nested_leak ADD COLUMN value String SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_nested_leak VALUES (10, [30], ['!'], '10')"
cache_size t_nested_leak

echo "-- the entry of the detached part must be evicted once its object is gone"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_nested_leak DETACH PART 'all_1_1_0'"
wait_for_cache_size t_nested_leak 1

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_nested_leak"
