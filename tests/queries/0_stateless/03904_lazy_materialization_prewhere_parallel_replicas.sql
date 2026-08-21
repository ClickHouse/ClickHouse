-- Regression test for block structure mismatch in UnionStep when lazy materialization,
-- PREWHERE, and parallel replicas interact. PREWHERE adds extra columns to ReadFromMergeTree
-- output that pass through expression DAGs and pollute the header of the local replica plan,
-- while remote replicas produce a different header.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 10;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;

DROP TABLE IF EXISTS t_lazy_mat_prewhere_parallel;
CREATE TABLE t_lazy_mat_prewhere_parallel (a UInt64, b UInt64, c UInt64, d UInt64)
ENGINE = MergeTree() PARTITION BY b ORDER BY a;
INSERT INTO t_lazy_mat_prewhere_parallel SELECT number, number % 2, number, number % 3 FROM numbers(0, 100);
INSERT INTO t_lazy_mat_prewhere_parallel SELECT number, number % 2, number, number % 3 FROM numbers(100, 100);

-- { echoOn }
-- The failing query from the stress test: aliases with expressions + WHERE that becomes PREWHERE
SELECT a + 1 AS a, b AS b, c + 1 AS c, d + 1 AS d FROM t_lazy_mat_prewhere_parallel WHERE d > 1 ORDER BY c LIMIT 3;
-- Simpler variants
SELECT * FROM t_lazy_mat_prewhere_parallel WHERE d > 1 ORDER BY c LIMIT 3;
SELECT a, b, c, d FROM t_lazy_mat_prewhere_parallel WHERE d > 1 ORDER BY c LIMIT 3;
-- Explicit PREWHERE
SELECT a + 1 AS a, b AS b, c + 1 AS c, d + 1 AS d FROM t_lazy_mat_prewhere_parallel PREWHERE d > 1 ORDER BY c LIMIT 3;
SELECT * FROM t_lazy_mat_prewhere_parallel PREWHERE d > 1 ORDER BY c LIMIT 3;
-- { echoOff }

DROP TABLE IF EXISTS t_lazy_mat_prewhere_parallel;
