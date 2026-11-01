-- An empty `MergeTree` table is read by `ReadNothingStep`, which is estimated as 0 rows. If a hash join with the empty
-- table on the right side goes through join reordering, it gets this estimate. The estimate is below
-- `parallel_hash_join_threshold`, so the join builds with the serial layout, whatever its kind.
-- A serial build has one `FillingRightJoinSide`, a parallel one has `max_threads` of them.

SET max_threads = 4, max_threads_min_free_memory_per_thread = 0;
SET join_algorithm = 'hash', parallel_hash_join_threshold = 100000;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET query_plan_join_swap_table = 0, query_plan_join_shard_by_pk_ranges = 0, query_plan_read_in_order_through_join = 0;
SET query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0;
SET use_hash_table_stats_for_join_reordering = 0, collect_hash_table_stats_during_joins = 0;

CREATE TABLE t_l (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_e (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_l SELECT number, number FROM numbers(10);

SELECT 'inner';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT * FROM t_l AS l INNER JOIN t_e AS r ON l.k = r.k)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'left';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT * FROM t_l AS l LEFT JOIN t_e AS r ON l.k = r.k)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'full';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT * FROM t_l AS l FULL JOIN t_e AS r ON l.k = r.k)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'inner, parallel_hash_join_threshold = 0';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT * FROM t_l AS l INNER JOIN t_e AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 0)
WHERE explain LIKE '%FillingRightJoinSide%';
