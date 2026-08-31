-- `join_algorithm = 'auto'` can fill the hash phase in parallel, then drain onto
-- `MergeJoin` when `max_rows_in_join` trips. Probe after the drain must stay
-- correct for INNER/LEFT/RIGHT/FULL, including unmatched right rows.
-- Pin a small `max_block_size` so the left side is several blocks after the drain.
-- Several `JoiningTransform`s then probe `MergeJoin` under `ExclusiveJoinResult`.
-- Random settings limits: max_rows_in_join=(50, 50); max_bytes_in_join=(0, 0); max_bytes_before_external_join=(0, 0); max_bytes_ratio_before_external_join=(0, 0); max_block_size=(16, 16)

SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET join_algorithm = 'auto';
SET parallel_hash_join_threshold = 1;
SET max_threads = 8;
SET max_block_size = 16;
SET max_rows_in_join = 50;
SET max_bytes_in_join = 0;
SET join_use_nulls = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t05046_l;
DROP TABLE IF EXISTS t05046_r;
CREATE TABLE t05046_l (n UInt64, s String) ENGINE = MergeTree ORDER BY n;
CREATE TABLE t05046_r (n UInt64, s String) ENGINE = MergeTree ORDER BY n;
INSERT INTO t05046_l SELECT number, 'l' FROM numbers(200);
INSERT INTO t05046_l SELECT number + 1000, 'lx' FROM numbers(10);
INSERT INTO t05046_r SELECT number, 'r' FROM numbers(200);
INSERT INTO t05046_r SELECT number + 2000, 'ry' FROM numbers(10);

SELECT 'fillers';
SELECT countIf(explain LIKE '%FillingRightJoinSide%') > 1
FROM (
    EXPLAIN PIPELINE
    SELECT t1.n FROM t05046_l AS t1 INNER JOIN t05046_r AS t2 ON t1.n = t2.n
    SETTINGS max_threads = 8, max_block_size = 16, query_plan_join_shard_by_pk_ranges = 0, join_algorithm = 'auto', parallel_hash_join_threshold = 1, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0
);

SELECT 'inner';
SELECT count() FROM t05046_l AS t1 INNER JOIN t05046_r AS t2 ON t1.n = t2.n
SETTINGS max_threads = 8, max_block_size = 16, query_plan_join_shard_by_pk_ranges = 0, join_algorithm = 'auto', parallel_hash_join_threshold = 1, max_rows_in_join = 50, max_bytes_in_join = 0, max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, query_plan_optimize_join_order_randomize = 0, log_comment = '05046_switch';

SELECT 'left';
SELECT count() FROM t05046_l AS t1 LEFT JOIN t05046_r AS t2 ON t1.n = t2.n
SETTINGS max_threads = 8, max_block_size = 16, query_plan_join_shard_by_pk_ranges = 0, join_algorithm = 'auto', parallel_hash_join_threshold = 1, max_rows_in_join = 50, max_bytes_in_join = 0, max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, query_plan_optimize_join_order_randomize = 0;

SELECT 'right';
SELECT count() FROM t05046_l AS t1 RIGHT JOIN t05046_r AS t2 ON t1.n = t2.n
SETTINGS max_threads = 8, max_block_size = 16, query_plan_join_shard_by_pk_ranges = 0, join_algorithm = 'auto', parallel_hash_join_threshold = 1, max_rows_in_join = 50, max_bytes_in_join = 0, max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, query_plan_optimize_join_order_randomize = 0;

SELECT 'full';
SELECT count() FROM t05046_l AS t1 FULL JOIN t05046_r AS t2 ON t1.n = t2.n
SETTINGS max_threads = 8, max_block_size = 16, query_plan_join_shard_by_pk_ranges = 0, join_algorithm = 'auto', parallel_hash_join_threshold = 1, max_rows_in_join = 50, max_bytes_in_join = 0, max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, query_plan_optimize_join_order_randomize = 0;

SYSTEM FLUSH LOGS query_log, text_log;
SET max_rows_to_read = 0;
SELECT 'switch';
SELECT count() > 0
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND message LIKE '%switching to PartialMergeJoin%'
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE log_comment = '05046_switch' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
      );

DROP TABLE t05046_l;
DROP TABLE t05046_r;
