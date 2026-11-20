-- Projections, including the implicit `_minmax_count_projection`, are used with parallel replicas
-- when `optimize_aggregation_in_order` is enabled: https://github.com/ClickHouse/ClickHouse/issues/123061

DROP TABLE IF EXISTS t_pr_proj_aio;
DROP TABLE IF EXISTS t_pr_proj_aio_sorted;
DROP TABLE IF EXISTS t_pr_proj_aio_normal;
DROP TABLE IF EXISTS t_pr_proj_aio_part;

CREATE TABLE t_pr_proj_aio (k UInt64, v UInt64, PROJECTION p (SELECT k, sum(v) GROUP BY k)) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_pr_proj_aio SELECT number % 10, number FROM numbers(100000);
INSERT INTO t_pr_proj_aio SELECT number % 10, number FROM numbers(100000, 100000);

CREATE TABLE t_pr_proj_aio_sorted (k UInt64, v UInt64, PROJECTION p (SELECT k, sum(v) GROUP BY k)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pr_proj_aio_sorted SELECT number % 10, number FROM numbers(100000);
INSERT INTO t_pr_proj_aio_sorted SELECT number % 10, number FROM numbers(100000, 100000);

CREATE TABLE t_pr_proj_aio_normal (k UInt64, v UInt64, PROJECTION p (SELECT * ORDER BY k)) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_pr_proj_aio_normal SELECT number % 10, number FROM numbers(100000);
INSERT INTO t_pr_proj_aio_normal SELECT number % 10, number FROM numbers(100000, 100000);

CREATE TABLE t_pr_proj_aio_part (p Date, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO t_pr_proj_aio_part SELECT toDate('2020-09-01') + number % 3, number FROM numbers(100000);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1;
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 0;
SET optimize_aggregation_in_order = 1, enable_memory_bound_merging_of_aggregation_results = 1, distributed_aggregation_memory_efficient = 1;

-- The aggregate projection is sorted by `k`, the table is not.
SELECT k, sum(v) FROM t_pr_proj_aio GROUP BY k ORDER BY k SETTINGS force_optimize_projection = 1;
-- Both are sorted by `k`: the projection is still read, and not aggregated in order.
SELECT k, sum(v) FROM t_pr_proj_aio_sorted GROUP BY k ORDER BY k SETTINGS force_optimize_projection = 1;
SELECT count() FROM (EXPLAIN PIPELINE SELECT k, sum(v) FROM t_pr_proj_aio_sorted GROUP BY k SETTINGS parallel_replicas_plan_based = 0, force_optimize_projection = 1) WHERE explain ILIKE '%InOrder%';
-- A normal projection.
SELECT k, count() FROM t_pr_proj_aio_normal WHERE k = 3 GROUP BY k SETTINGS force_optimize_projection = 1;
-- `count()` from partition metadata.
SELECT count() FROM t_pr_proj_aio_part WHERE p = '2020-09-01' SETTINGS max_rows_to_read = 1;

DROP TABLE t_pr_proj_aio;
DROP TABLE t_pr_proj_aio_sorted;
DROP TABLE t_pr_proj_aio_normal;
DROP TABLE t_pr_proj_aio_part;
