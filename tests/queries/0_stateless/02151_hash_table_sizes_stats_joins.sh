#!/usr/bin/env bash
# Tags: long, distributed, no-debug, no-tsan, no-msan, no-ubsan, no-asan, no-random-settings, no-random-merge-tree-settings

# shellcheck disable=SC2154

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


opts=(
    --join_algorithm='parallel_hash'
)

$CLICKHOUSE_CLIENT -nq "
  CREATE TABLE t1(a UInt32, b UInt32) ENGINE=MergeTree ORDER BY ();
  INSERT INTO t1 SELECT number, number FROM numbers_mt(1e6);

  CREATE TABLE t2(a UInt32, b UInt32) ENGINE=MergeTree ORDER BY ();
  INSERT INTO t2 SELECT number, number FROM numbers_mt(1e6);
"

queries_without_preallocation=()
queries_with_preallocation=()

run_new_query() {
  query_id1="hash_table_sizes_stats_joins_$RANDOM$RANDOM"
  # when we see a query for the first time we only collect it stats when execution ends. preallocation will happen only on the next run
  queries_without_preallocation+=("$query_id1")
  $CLICKHOUSE_CLIENT "${opts[@]}" --query_id="$query_id1" -q "$1" --format Null

  query_id2="hash_table_sizes_stats_joins_$RANDOM$RANDOM"
  queries_with_preallocation+=("$query_id2")
  $CLICKHOUSE_CLIENT "${opts[@]}" --query_id="$query_id2" -q "$1" --format Null
}

run_new_query "SELECT * FROM t1 AS x INNER JOIN t2 AS y ON x.a = y.a"
# it only matters what columns from the right table are part of the join key, as soon as we change them - it is a new cache entry
run_new_query "SELECT * FROM t1 AS x INNER JOIN t2 AS y ON x.a = y.b"
run_new_query "SELECT * FROM t1 AS x INNER JOIN t2 AS y USING (a, b)"

# we already had a join on t2.a, so cache should be populated
query_id="hash_table_sizes_stats_joins_$RANDOM$RANDOM"
queries_with_preallocation+=("$query_id")
$CLICKHOUSE_CLIENT "${opts[@]}" --query_id="$query_id" -q "SELECT * FROM t1 AS x INNER JOIN t2 AS y ON x.b = y.a" --format Null
# the same query with a different alias for the t2
query_id="hash_table_sizes_stats_joins_$RANDOM$RANDOM"
queries_with_preallocation+=("$query_id")
$CLICKHOUSE_CLIENT "${opts[@]}" --query_id="$query_id" -q "SELECT * FROM t1 AS x INNER JOIN t2 AS z ON x.b = z.a" --format Null

# now t1 is the right table
run_new_query "SELECT * FROM t2 AS x INNER JOIN t1 AS y ON x.a = y.a"

##################################

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"

for i in "${!queries_without_preallocation[@]}"; do
  $CLICKHOUSE_CLIENT --param_query_id="${queries_without_preallocation[$i]}" -q "
    -- the old analyzer is not supported
    SELECT sum(if(getSetting('enable_analyzer'), ProfileEvents['HashJoinPreallocatedElementsInHashTables'] = 0, 1))
      FROM system.query_log
     WHERE event_date >= yesterday() AND query_id = {query_id:String} AND current_database = currentDatabase() AND type = 'QueryFinish'
  "
done

for i in "${!queries_with_preallocation[@]}"; do
  $CLICKHOUSE_CLIENT --param_query_id="${queries_with_preallocation[$i]}" -q "
    -- the old analyzer is not supported
    SELECT sum(if(getSetting('enable_analyzer'), ProfileEvents['HashJoinPreallocatedElementsInHashTables'] > 0, 1))
      FROM system.query_log
     WHERE event_date >= yesterday() AND query_id = {query_id:String} AND current_database = currentDatabase() AND type = 'QueryFinish'
  "
done
