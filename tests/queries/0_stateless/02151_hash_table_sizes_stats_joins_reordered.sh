#!/usr/bin/env bash
# Tags: long

# Regression test for HashTablesStatistics cache hints reaching joins whose right side is
# a sub-join built during join reorder (i.e., not present in the original query plan).
#
# Before the fix that derives cache keys for reorder-built sub-joins, a multi-table join
# such as `t1 JOIN t2 JOIN t3 JOIN t4` with the optimizer reordering tables would skip
# the per-join HashTablesStatistics lookup for any join whose right child was constructed
# inside `chooseJoinOrder`, because the pre-walk cache_keys map only had entries for the
# original tree's nodes. The result: even after a parallel_hash run populated the cache,
# subsequent runs could not preallocate the right-side hash table for those joins.
#
# The test forces parallel_hash, runs a 4-way join twice, and asserts that ALL right-side
# joins in the second run preallocate (i.e., HashJoinPreallocatedElementsInHashTables > 0).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

opts=(
    --enable_analyzer=1
    --join_algorithm='parallel_hash'
    # `query_plan_join_swap_table=true` forces every join to swap sides during reorder so the
    # right children are reorder-built sub-join nodes (rather than the left-deep originals).
    # This is precisely the case the fix targets — without `true` the tied row counts of the
    # four tables here can let the optimizer keep the syntactic order, weakening the test.
    --query_plan_join_swap_table=true
    # The test relies on join reorder building intermediate sub-joins, so explicitly
    # enable it: random settings can otherwise pick `query_plan_optimize_join_order_limit=0`
    # which keeps the original left-deep tree and yields prealloc=0 even with the fix.
    --query_plan_optimize_join_order_limit=64
)

# Use unique table names to avoid colliding with other tests sharing the test database.
# All four tables have the SAME row count (N) so each of the three sub-join hash tables
# allocates exactly N entries. The hot run's HashJoinPreallocatedElementsInHashTables
# event sums over preallocated right sides, so the exact value tells us precisely how
# many of the three sub-join builds reused the cache hint.
N=1000000
T1="reordered_t1"; T2="reordered_t2"; T3="reordered_t3"; T4="reordered_t4"

$CLICKHOUSE_CLIENT -q "
  DROP TABLE IF EXISTS $T1;
  DROP TABLE IF EXISTS $T2;
  DROP TABLE IF EXISTS $T3;
  DROP TABLE IF EXISTS $T4;

  CREATE TABLE $T1(a UInt32, b UInt32) ENGINE=MergeTree ORDER BY ();
  INSERT INTO $T1 SELECT number, number FROM numbers_mt($N);

  CREATE TABLE $T2(a UInt32, b UInt32) ENGINE=MergeTree ORDER BY ();
  INSERT INTO $T2 SELECT number, number FROM numbers_mt($N);

  CREATE TABLE $T3(a UInt32, b UInt32) ENGINE=MergeTree ORDER BY ();
  INSERT INTO $T3 SELECT number, number FROM numbers_mt($N);

  CREATE TABLE $T4(a UInt32, b UInt32) ENGINE=MergeTree ORDER BY ();
  INSERT INTO $T4 SELECT number, number FROM numbers_mt($N);
"

# 4-way join — there are three sub-joins; at least the upper two have a right child that
# was built during reorder (i.e. not present in the original tree). All four tables share
# the same key range [0, N), so every sub-join produces exactly N rows and every right-side
# hash table allocates exactly N entries — the exact prealloc value pins down how many of
# the three sub-joins reused the cache hint.
SQL="SELECT count() FROM $T1 INNER JOIN $T2 ON $T1.a = $T2.a INNER JOIN $T3 ON $T2.a = $T3.a INNER JOIN $T4 ON $T3.a = $T4.a"

cold_id="hash_table_sizes_stats_joins_reordered_cold_$RANDOM$RANDOM"
hot_id="hash_table_sizes_stats_joins_reordered_hot_$RANDOM$RANDOM"

$CLICKHOUSE_CLIENT "${opts[@]}" --query_id="$cold_id" -q "$SQL" --format Null
$CLICKHOUSE_CLIENT "${opts[@]}" --query_id="$hot_id"  -q "$SQL" --format Null

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# Cold run: nothing in the cache yet -> no preallocation expected.
$CLICKHOUSE_CLIENT --param_query_id="$cold_id" -q "
  SELECT if(any(ProfileEvents['HashJoinPreallocatedElementsInHashTables']) = 0, '1', 'Error: ' || any(query_id) || ' got prealloc=' || toString(any(ProfileEvents['HashJoinPreallocatedElementsInHashTables'])))
    FROM system.query_log
   WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = {query_id:String} AND current_database = currentDatabase() AND type = 'QueryFinish'
"

# Hot run: assert that ALL THREE sub-joins reused the cache hint. Because every table is N
# rows and joins on the same key range, every preallocated hash table is exactly N entries,
# so the sum is 3*N when the fix is in place. Any partial fix that leaves one sub-join's
# right side without a hint would yield 2*N or N, which does not satisfy the equality.
$CLICKHOUSE_CLIENT --param_query_id="$hot_id" --param_expected_prealloc=$((3 * N)) -q "
  SELECT if(any(ProfileEvents['HashJoinPreallocatedElementsInHashTables']) = {expected_prealloc:UInt64}, '1', 'Error: ' || any(query_id) || ' got prealloc=' || toString(any(ProfileEvents['HashJoinPreallocatedElementsInHashTables'])))
    FROM system.query_log
   WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = {query_id:String} AND current_database = currentDatabase() AND type = 'QueryFinish'
"

$CLICKHOUSE_CLIENT -q "
  DROP TABLE $T1;
  DROP TABLE $T2;
  DROP TABLE $T3;
  DROP TABLE $T4;
"
