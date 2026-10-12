-- The hash phase of `join_algorithm = 'auto'` follows `parallel_hash_join_threshold` like a bare `HashJoin`.
--
-- Join-order stats stay on so the planner AUTO path gets MergeTree `totalRows` (200).
-- `05045_missing_estimate_parallel` turns join-order off: no rhs estimate, high threshold still parallel.

SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 0;
SET query_plan_join_shard_by_pk_ranges = 0;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET max_threads = 16;

DROP TABLE IF EXISTS t05045_l;
DROP TABLE IF EXISTS t05045_r;
CREATE TABLE t05045_l (n UInt64) ENGINE = MergeTree ORDER BY n;
CREATE TABLE t05045_r (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t05045_l SELECT number FROM numbers(100);
INSERT INTO t05045_r SELECT number FROM numbers(200);

SET join_algorithm = 'auto';
SET parallel_hash_join_threshold = 100000;
SELECT t1.n FROM t05045_l AS t1 INNER JOIN t05045_r AS t2 ON t1.n = t2.n
SETTINGS log_comment = '05045_join_switcher_serial' FORMAT Null;

SET parallel_hash_join_threshold = 1;
SELECT t1.n FROM t05045_l AS t1 INNER JOIN t05045_r AS t2 ON t1.n = t2.n
SETTINGS log_comment = '05045_join_switcher_parallel' FORMAT Null;

SET join_algorithm = 'hash';
SET parallel_hash_join_threshold = 100000;
SELECT t1.n FROM t05045_l AS t1 INNER JOIN t05045_r AS t2 ON t1.n = t2.n
SETTINGS query_plan_optimize_join_order_limit = 0, log_comment = '05045_missing_estimate_parallel' FORMAT Null;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] AS serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] AS parallel
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05045_join_switcher_serial', '05045_join_switcher_parallel', '05045_missing_estimate_parallel')
ORDER BY event_time_microseconds;

DROP TABLE t05045_l;
DROP TABLE t05045_r;
