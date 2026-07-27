#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every pair below runs the same query twice: the first run populates the hash-table-stats cache,
# the second one (-prealloc) must preallocate from it, which is checked through the
# AggregationPreallocatedElementsInHashTables profile event. Different pairs use different cache
# keys (that is the point of the test), so they are independent: to keep the test fast on slow
# (debug/sanitizer) builds, the pairs run in parallel, and only the two runs within a pair are
# sequential. For the same reason logs are flushed and queried once, at the end; each section uses
# its own query_id prefix so the report queries do not pick up each other's rows.
#
# The group counts (650e3 / 520e3) must stay above the 500e3 lower bound under which getSizeHint
# does not preallocate at all (with a single hash table, i.e. max_threads = 1) - do not shrink
# them further. The FINAL / SAMPLE modifiers case lives in
# 04613_hash_table_sizes_stats_table_expression_modifiers, in a separate test to keep single runs
# short for the flaky check.

run_pair()
{
    local query_id_prefix=$1 name=$2 query=$3
    shift 3
    $CLICKHOUSE_CLIENT "$@" --query_id="${query_id_prefix}_${name}" -q "$query"
    $CLICKHOUSE_CLIENT "$@" --query_id="${query_id_prefix}_${name}-prealloc" -q "$query"
}

settings=(
    --max_threads=1
    --enable_analyzer=1
    --collect_hash_table_stats_during_aggregation=1
    --max_size_to_preallocate_for_aggregation=1000000000000
)

big_query="SELECT number AS k FROM numbers(650e3) GROUP BY k FORMAT Null"
small_query="SELECT number AS k FROM numbers(520e3) GROUP BY k FORMAT Null"

local_prefix="${CLICKHOUSE_DATABASE}_04509_local_$RANDOM$RANDOM"
dist_prefix="${CLICKHOUSE_DATABASE}_04509_dist_$RANDOM$RANDOM"

run_pair "$local_prefix" big "$big_query" "${settings[@]}" &
run_pair "$local_prefix" small "$small_query" "${settings[@]}" &

# The same, but through a serialized query plan (distributed execution): the read is reconstructed
# on the shard by resolveStorages / ReadFromTableFunctionStep, which must also carry the table
# function subtree so numbers(650e3) and numbers(520e3) do not share one stats entry. The
# preallocation happens in the shard-side (secondary) aggregation, so it is read from the secondary
# query rows (is_initial_query = 0). process_query_plan_packet is enabled in the test server config.
dist_settings=(
    --max_threads=1
    --enable_analyzer=1
    --collect_hash_table_stats_during_aggregation=1
    --max_size_to_preallocate_for_aggregation=1000000000000
    --serialize_query_plan=1
    --prefer_localhost_replica=0
)

dist_big_query="SELECT number AS k FROM cluster('test_shard_localhost', numbers(650e3)) GROUP BY k FORMAT Null"
dist_small_query="SELECT number AS k FROM cluster('test_shard_localhost', numbers(520e3)) GROUP BY k FORMAT Null"

run_pair "$dist_prefix" dist-big "$dist_big_query" "${dist_settings[@]}" &
run_pair "$dist_prefix" dist-small "$dist_small_query" "${dist_settings[@]}" &

wait

# The labels within each section happen to sort in the order the reference expects, and unlike
# event_time the sort is stable under the parallel execution above.
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;

    SELECT replace(query_id, '${local_prefix}_', ''), ProfileEvents['AggregationPreallocatedElementsInHashTables']
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND startsWith(query_id, '$local_prefix')
    AND endsWith(query_id, '-prealloc')
    ORDER BY 1;

    SELECT replace(initial_query_id, '${dist_prefix}_', ''), ProfileEvents['AggregationPreallocatedElementsInHashTables']
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND is_initial_query = 0
    AND startsWith(initial_query_id, '$dist_prefix')
    AND endsWith(initial_query_id, '-prealloc')
    ORDER BY 1;
"
