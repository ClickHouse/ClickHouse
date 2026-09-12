#!/usr/bin/env bash
# Tags: long

# `join_algorithm='hash'` must still compute cache keys (layout selection ignores the list).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

opts=(
    --enable_analyzer=1
    --join_algorithm='hash'
    # Cache keys are stamped by join-order optimization; keep its plan stable across both runs.
    --query_plan_optimize_join_order_limit=10
    --query_plan_optimize_join_order_randomize=0
)

# Large enough that the size estimate selects the parallel layout.
# `reserveSlot` sums `hint / num_slots` per slot; a hint not divisible by num_slots (power of two,
# at most 256) would truncate the expected total.
N=1024000
T1="join_hash_cache_stats_t1"; T2="join_hash_cache_stats_t2"

$CLICKHOUSE_CLIENT -q "
  DROP TABLE IF EXISTS $T1;
  DROP TABLE IF EXISTS $T2;

  CREATE TABLE $T1(a UInt32) ENGINE=MergeTree ORDER BY ();
  INSERT INTO $T1 SELECT number FROM numbers_mt($N);

  CREATE TABLE $T2(a UInt32) ENGINE=MergeTree ORDER BY ();
  INSERT INTO $T2 SELECT number FROM numbers_mt($N);
"

SQL="SELECT count() FROM $T1 INNER JOIN $T2 ON $T1.a = $T2.a"

cold_id="join_hash_algorithm_cache_stats_cold_$RANDOM$RANDOM"
hot_id="join_hash_algorithm_cache_stats_hot_$RANDOM$RANDOM"

$CLICKHOUSE_CLIENT "${opts[@]}" --query_id="$cold_id" -q "$SQL" --format Null
$CLICKHOUSE_CLIENT "${opts[@]}" --query_id="$hot_id"  -q "$SQL" --format Null

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT --param_query_id="$cold_id" -q "
  SELECT if(any(ProfileEvents['HashJoinPreallocatedElementsInHashTables']) = 0, '1', 'Error: ' || any(query_id) || ' got prealloc=' || toString(any(ProfileEvents['HashJoinPreallocatedElementsInHashTables'])))
    FROM system.query_log
   WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = {query_id:String} AND current_database = currentDatabase() AND type = 'QueryFinish'
"

$CLICKHOUSE_CLIENT --param_query_id="$hot_id" --param_expected_prealloc=$N -q "
  SELECT if(any(ProfileEvents['HashJoinPreallocatedElementsInHashTables']) = {expected_prealloc:UInt64}, '1', 'Error: ' || any(query_id) || ' got prealloc=' || toString(any(ProfileEvents['HashJoinPreallocatedElementsInHashTables'])))
    FROM system.query_log
   WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = {query_id:String} AND current_database = currentDatabase() AND type = 'QueryFinish'
"

$CLICKHOUSE_CLIENT -q "
  DROP TABLE $T1;
  DROP TABLE $T2;
"
