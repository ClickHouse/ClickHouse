#!/usr/bin/env bash
# Tags: long, no-debug, no-tsan, no-msan, no-ubsan, no-asan, no-random-settings, no-random-merge-tree-settings

# shellcheck disable=SC2154,SC2162

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# tests rely on that all the rows are unique and max_threads divides table_size
table_size=1000005
max_threads=5


prepare_table() {
  table_name="t_hash_table_sizes_stats_$RANDOM$RANDOM"
  $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table_name;"
  if [ -z "$1" ]; then
    $CLICKHOUSE_CLIENT -q "CREATE TABLE $table_name(number UInt64) Engine=MergeTree() ORDER BY tuple() SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';"
  else
    $CLICKHOUSE_CLIENT -q "CREATE TABLE $table_name(number UInt64) Engine=MergeTree() ORDER BY $1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';"
  fi
  $CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES $table_name;"
  for ((i = 1; i <= max_threads; i++)); do
    cnt=$((table_size / max_threads))
    from=$(((i - 1) * cnt))
    $CLICKHOUSE_CLIENT -q "INSERT INTO $table_name SELECT * FROM numbers($from, $cnt);"
  done
}

prepare_table_with_sorting_key() {
  prepare_table "$1"
}

run_query() {
  query_id="${CLICKHOUSE_DATABASE}_hash_table_sizes_stats_$RANDOM$RANDOM"
  $CLICKHOUSE_CLIENT --query_id="$query_id" -q "
    SET max_block_size = $((table_size / 10));
    SET merge_tree_min_rows_for_concurrent_read = 1;
    SET max_untracked_memory = 0;
    SET max_size_to_preallocate_for_aggregation = 1e12;
    $query"
}

check_preallocated_elements() {
  # rows may be distributed in any way including "everything goes to the one particular thread"
  $CLICKHOUSE_CLIENT --param_query_id="$1" -q "
    SELECT COUNT(*)
      FROM system.query_log
     WHERE event_date >= yesterday() AND query_id = {query_id:String} AND current_database = currentDatabase()
           AND ProfileEvents['AggregationPreallocatedElementsInHashTables'] BETWEEN $2 AND $3"
}

check_convertion_to_two_level() {
  # rows may be distributed in any way including "everything goes to the one particular thread"
  $CLICKHOUSE_CLIENT --param_query_id="$1" -q "
    SELECT SUM(ProfileEvents['AggregationHashTablesInitializedAsTwoLevel']) BETWEEN 1 AND $max_threads
      FROM system.query_log
     WHERE event_date >= yesterday() AND query_id = {query_id:String} AND current_database = currentDatabase()"
}

print_border() {
  echo "--"
}

# each test case appends to this array
expected_results=()

check_expectations() {
  $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"

  for i in "${!expected_results[@]}"; do
    read -a args <<< "${expected_results[$i]}"
    if [ ${#args[@]} -eq 4 ]; then
      check_convertion_to_two_level "${args[0]}"
    fi
    check_preallocated_elements "${args[@]}"
    print_border
  done
}

# shellcheck source=./02151_hash_table_sizes_stats.testcases
source "$CURDIR"/02151_hash_table_sizes_stats.testcases

test_one_thread_simple_group_by
test_one_thread_simple_group_by_with_limit
test_one_thread_simple_group_by_with_join_and_subquery
test_several_threads_simple_group_by_with_limit_single_level_ht
test_several_threads_simple_group_by_with_limit_two_level_ht
test_several_threads_simple_group_by_with_limit_and_rollup_single_level_ht
test_several_threads_simple_group_by_with_limit_and_rollup_two_level_ht
test_several_threads_simple_group_by_with_limit_and_cube_single_level_ht
test_several_threads_simple_group_by_with_limit_and_cube_two_level_ht

check_expectations
