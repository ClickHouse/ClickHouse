#!/usr/bin/env bash
# Tags: long

# A `hash` build sizes its hash table from the hash table statistics cache, as `hash` does. The first
# run of a query publishes its exact distinct-key count; the second run reads that count back, counts it in
# `HashJoinPreallocatedElementsInHashTables` (0 on the first run, exactly the key count on the second) and starts
# from a table that holds every key, so the table never grows during that build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

opts=(
    --enable_analyzer=1
    --join_algorithm='hash'
    --max_bytes_before_external_join=0
    --max_bytes_ratio_before_external_join=0
    # Cache keys are stamped by join-order optimization; keep its plan stable across both runs.
    --query_plan_optimize_join_order_limit=10
    --query_plan_optimize_join_order_randomize=0
)

N=1024000
T1="join_partitioned_cache_stats_t1"; T2="join_partitioned_cache_stats_t2"

$CLICKHOUSE_CLIENT -q "
  DROP TABLE IF EXISTS $T1;
  DROP TABLE IF EXISTS $T2;

  CREATE TABLE $T1(a UInt32) ENGINE=MergeTree ORDER BY ();
  INSERT INTO $T1 SELECT number FROM numbers_mt($N);

  CREATE TABLE $T2(a UInt32) ENGINE=MergeTree ORDER BY ();
  INSERT INTO $T2 SELECT number FROM numbers_mt($N);
"

SQL="SELECT count() FROM $T1 INNER JOIN $T2 ON $T1.a = $T2.a"

cold_id="join_partitioned_cache_stats_cold_$RANDOM$RANDOM"
hot_id="join_partitioned_cache_stats_hot_$RANDOM$RANDOM"

$CLICKHOUSE_CLIENT "${opts[@]}" --query_id="$cold_id" -q "$SQL" --format Null
$CLICKHOUSE_CLIENT "${opts[@]}" --query_id="$hot_id"  -q "$SQL" --format Null

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

echo "-- the first run has no cached count"
$CLICKHOUSE_CLIENT --param_query_id="$cold_id" -q "
  SELECT if(any(ProfileEvents['HashJoinPreallocatedElementsInHashTables']) = 0, '1', 'Error: ' || any(query_id) || ' got prealloc=' || toString(any(ProfileEvents['HashJoinPreallocatedElementsInHashTables'])))
    FROM system.query_log
   WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = {query_id:String} AND current_database = currentDatabase() AND type = 'QueryFinish'
"

echo "-- the second run sizes its table from the cached count and never grows it"
$CLICKHOUSE_CLIENT --param_query_id="$hot_id" --param_expected_prealloc=$N -q "
  SELECT if(any(ProfileEvents['HashJoinPreallocatedElementsInHashTables']) = {expected_prealloc:UInt64}, '1', 'Error: ' || any(query_id) || ' got prealloc=' || toString(any(ProfileEvents['HashJoinPreallocatedElementsInHashTables']))),
         if(any(ProfileEvents['HashJoinTableResizes']) = 0, '1', 'Error: ' || any(query_id) || ' got resizes=' || toString(any(ProfileEvents['HashJoinTableResizes'])))
    FROM system.query_log
   WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = {query_id:String} AND current_database = currentDatabase() AND type = 'QueryFinish'
"

$CLICKHOUSE_CLIENT -q "
  DROP TABLE $T1;
  DROP TABLE $T2;
"
