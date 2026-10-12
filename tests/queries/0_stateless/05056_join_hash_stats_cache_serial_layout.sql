-- Warm `HashTablesStatistics` can still make a hash join go serial.
-- `use_statistics = 0` and `ORDER BY ()` keep the right-table estimate at
-- `totalRows` (and a pushed filter is treated as unknown), so the cold run is
-- parallel. It leaves the smaller filtered size in the cache, and the same
-- query then builds with the serial layout.
-- Random settings limits: max_threads=(8, 8); parallel_hash_join_threshold=(5001, 5001); collect_hash_table_stats_during_joins=(1, 1); use_hash_table_stats_for_join_reordering=(1, 1); use_statistics=(0, 0)

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = false;
SET query_plan_join_shard_by_pk_ranges = 0;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET use_query_condition_cache = 0;
SET use_statistics = 0;
SET collect_hash_table_stats_during_joins = 1;
SET use_hash_table_stats_for_join_reordering = 1;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET join_algorithm = 'hash';
SET parallel_hash_join_threshold = 5001;
SET max_threads = 8;

DROP TABLE IF EXISTS t05056_l;
DROP TABLE IF EXISTS t05056_r;
CREATE TABLE t05056_l (a UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t05056_r (a UInt64) ENGINE = MergeTree ORDER BY ();
INSERT INTO t05056_l SELECT number FROM numbers(20000);
INSERT INTO t05056_r SELECT number FROM numbers(10000);

SELECT count() FROM t05056_l AS t1 INNER JOIN t05056_r AS t2 ON t1.a = t2.a WHERE t2.a < 1000
SETTINGS log_comment = '05056_cold' FORMAT Null;

SELECT count() FROM t05056_l AS t1 INNER JOIN t05056_r AS t2 ON t1.a = t2.a WHERE t2.a < 1000
SETTINGS log_comment = '05056_warm' FORMAT Null;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] AS serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] AS parallel
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05056_cold', '05056_warm')
ORDER BY event_time_microseconds;

DROP TABLE t05056_l;
DROP TABLE t05056_r;
