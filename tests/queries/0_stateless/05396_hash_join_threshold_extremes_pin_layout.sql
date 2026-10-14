-- `parallel_hash_join_threshold = 0` always builds a hash join with the parallel layout, and the largest value
-- always builds it with the serial layout, also when the right side has no row estimate. Every other value
-- builds a join without an estimate with the parallel layout.

SET max_threads = 4, max_threads_min_free_memory_per_thread = 0;
SET join_algorithm = 'hash';
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET query_plan_join_swap_table = 0, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_randomize = 0;
SET use_hash_table_stats_for_join_reordering = 0, collect_hash_table_stats_during_joins = 0;

CREATE TABLE t_l (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_r (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_l SELECT number FROM numbers(10);
INSERT INTO t_r SELECT number FROM numbers(10);

-- With `query_plan_optimize_join_order_limit = 0` no join gets an estimate.
SELECT * FROM t_l AS l INNER JOIN t_r AS r ON l.k = r.k FORMAT Null SETTINGS query_plan_optimize_join_order_limit = 0,
    parallel_hash_join_threshold = 18446744073709551615, log_comment = '05396_no_estimate_largest';
SELECT * FROM t_l AS l INNER JOIN t_r AS r ON l.k = r.k FORMAT Null SETTINGS query_plan_optimize_join_order_limit = 0,
    parallel_hash_join_threshold = 0, log_comment = '05396_no_estimate_0';
SELECT * FROM t_l AS l INNER JOIN t_r AS r ON l.k = r.k FORMAT Null SETTINGS query_plan_optimize_join_order_limit = 0,
    parallel_hash_join_threshold = 100000, log_comment = '05396_no_estimate_100000';

-- The join order optimization gives the right side its estimate of 10 rows.
SELECT * FROM t_l AS l INNER JOIN t_r AS r ON l.k = r.k FORMAT Null SETTINGS query_plan_optimize_join_order_limit = 10,
    parallel_hash_join_threshold = 18446744073709551615, log_comment = '05396_estimate_largest';
SELECT * FROM t_l AS l INNER JOIN t_r AS r ON l.k = r.k FORMAT Null SETTINGS query_plan_optimize_join_order_limit = 10,
    parallel_hash_join_threshold = 0, log_comment = '05396_estimate_0';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] AS serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] AS parallel
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05396_no_estimate_largest',
    '05396_no_estimate_0', '05396_no_estimate_100000', '05396_estimate_largest', '05396_estimate_0')
ORDER BY event_time_microseconds;

DROP TABLE t_l;
DROP TABLE t_r;
