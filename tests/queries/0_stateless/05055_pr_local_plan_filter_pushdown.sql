-- A filter that sits above the part of the query parallel replicas execute must reach the
-- initiator's own local plan and become a `PREWHERE` there, without `parallel_replicas_filter_pushdown`:
-- that setting only governs the query text shipped to the remote replicas.

DROP TABLE IF EXISTS t_pr_local_pd;
DROP VIEW IF EXISTS v_pr_local_pd;

CREATE TABLE t_pr_local_pd (a UInt32, b String) ENGINE = MergeTree ORDER BY a;
CREATE VIEW v_pr_local_pd AS SELECT * FROM t_pr_local_pd;
INSERT INTO t_pr_local_pd SELECT number, toString(number) FROM numbers(1000);

-- For runs with the old analyzer
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;
-- Read the outer query over the view, so that the filter starts above the parallel replicas read.
SET parallel_replicas_allow_view_over_mergetree = 0;
SET parallel_replicas_plan_based = 0;
-- The plan below asserts that the filter reaches the read step as a `PREWHERE`, so pin the two
-- optimizations that fold it in.
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;
-- `parallel_replicas_filter_pushdown` is left at its default.

SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_pr_local_pd WHERE a = 5)
WHERE explain LIKE '%Prewhere filter column%';

SELECT * FROM v_pr_local_pd WHERE a = 5;

DROP VIEW v_pr_local_pd;
DROP TABLE t_pr_local_pd;
