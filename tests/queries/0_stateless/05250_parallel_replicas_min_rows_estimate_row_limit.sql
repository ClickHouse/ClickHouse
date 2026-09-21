-- The replica-count estimate taken when parallel_replicas_min_number_of_rows_per_replica is set
-- must not enforce the throwing read row limits: it charges whole granules of the base table, while
-- the read that executes may be served by a projection or bounded by its read order.

SET use_statistics_for_part_pruning = 0;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_only_with_analyzer = 0;
SET optimize_use_projections = 1, optimize_aggregation_in_order = 0;
SET enable_parallel_replicas = 2, parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_min_number_of_rows_per_replica = 1;

DROP TABLE IF EXISTS x;
CREATE TABLE x (i int) ENGINE = MergeTree ORDER BY i SETTINGS index_granularity = 3;
SYSTEM STOP MERGES x;
INSERT INTO x SELECT number FROM numbers(10);

SELECT '--- implicit projection serves the query within the budget ---';
SELECT max(i) FROM x SETTINGS max_rows_to_read = 2, optimize_use_implicit_projections = 1;
SELECT max(i) FROM x SETTINGS max_rows_to_read_leaf = 2, optimize_use_implicit_projections = 1;

SELECT '--- the limit still applies to the read that executes ---';
-- sum() cannot be answered from _minmax_count_projection, so the base table is read and must throw.
SELECT sum(i) FROM x SETTINGS max_rows_to_read = 2, optimize_use_implicit_projections = 1 FORMAT Null; -- { serverError TOO_MANY_ROWS }
SELECT max(i) FROM x SETTINGS max_rows_to_read = 2, optimize_use_implicit_projections = 0 FORMAT Null; -- { serverError TOO_MANY_ROWS }

SELECT '--- unchanged without parallel replicas ---';
SELECT max(i) FROM x SETTINGS max_rows_to_read = 2, optimize_use_implicit_projections = 1, enable_parallel_replicas = 0;

DROP TABLE x;

DROP TABLE IF EXISTS t;
CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4;
SYSTEM STOP MERGES t;
INSERT INTO t SELECT number FROM numbers(100);

SELECT '--- read order bounds the rows the query needs ---';
SET use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 0, use_skip_indexes_for_top_k = 1, optimize_read_in_order = 1, max_threads = 1;
SELECT a FROM t ORDER BY a LIMIT 5 SETTINGS max_rows_to_read = 12;
SELECT a FROM t ORDER BY a LIMIT 20 FORMAT Null SETTINGS max_rows_to_read = 12; -- { serverError TOO_MANY_ROWS }

DROP TABLE t;
