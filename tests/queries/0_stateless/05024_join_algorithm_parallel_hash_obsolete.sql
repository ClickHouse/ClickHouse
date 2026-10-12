-- `join_algorithm = 'parallel_hash'` is accepted without a warning and reads back as `hash`. It still
-- runs a hash join under `compatibility = '26.8'`. With the default list,
-- `parallel_hash_join_threshold` decides whether the join is built on one thread or on several.
--
-- `query_plan_optimize_join_order_limit = 10` gives the join an estimate of the
-- right table size (its row count). Without an estimate both thresholds below
-- would choose the parallel layout.

SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 0;
SET query_plan_join_shard_by_pk_ranges = 0;
SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET max_threads = 16;

SELECT 'default';
SELECT value FROM system.settings WHERE name = 'join_algorithm';

SET join_algorithm = 'parallel_hash';
SELECT value FROM system.settings WHERE name = 'join_algorithm';
SELECT count()
FROM system.warnings
WHERE message LIKE '%Obsolete setting%' AND message LIKE '%parallel_hash%';
SET join_algorithm = DEFAULT;

DROP TABLE IF EXISTS t05024_l;
DROP TABLE IF EXISTS t05024_r;
CREATE TABLE t05024_l (n UInt64) ENGINE = MergeTree ORDER BY n;
CREATE TABLE t05024_r (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t05024_l SELECT number FROM numbers(100);
INSERT INTO t05024_r SELECT number FROM numbers(200);

SET parallel_hash_join_threshold = 1;
SELECT t1.n FROM t05024_l AS t1 INNER JOIN t05024_r AS t2 ON t1.n = t2.n
SETTINGS log_comment = '05024_inner_parallel' FORMAT Null;

SET parallel_hash_join_threshold = 100000;
SELECT t1.n FROM t05024_l AS t1 INNER JOIN t05024_r AS t2 ON t1.n = t2.n
SETTINGS log_comment = '05024_inner_serial' FORMAT Null;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] AS serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] AS parallel
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05024_inner_parallel', '05024_inner_serial')
ORDER BY event_time_microseconds;

SELECT 'explicit_parallel_hash_join';
SELECT count()
FROM numbers(10) AS t1 INNER JOIN numbers(10) AS t2 ON t1.number = t2.number
SETTINGS compatibility = '26.8', join_algorithm = 'parallel_hash';

DROP TABLE t05024_l;
DROP TABLE t05024_r;
