-- An ordinary filter above the part of the query parallel replicas execute reaches the initiator's
-- local plan only together with the replicas, that is only when `parallel_replicas_filter_pushdown`
-- also splices it into their query. On its own it could fix a sort key column and make the initiator
-- read in order while the replicas do not, see 05057. A join runtime filter is not like that and goes
-- into the local plan regardless of the setting, see 05056.

DROP TABLE IF EXISTS t_pr_local_pd;
DROP VIEW IF EXISTS v_pr_local_pd;

CREATE TABLE t_pr_local_pd (a UInt32, b String) ENGINE = MergeTree ORDER BY a;
-- Read the outer query over the view, so that the filter starts above the parallel replicas read.
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
SET parallel_replicas_allow_view_over_mergetree = 0;
SET parallel_replicas_plan_based = 0;
-- The plan below is about the filter reaching the read step as a `PREWHERE`, so pin the two
-- optimizations that fold it in.
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;

SELECT 'default: not in the local plan';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_pr_local_pd WHERE a = 5)
WHERE explain LIKE '%Prewhere filter column%';
SELECT * FROM v_pr_local_pd WHERE a = 5;

SELECT 'enabled: in the local plan';
SET parallel_replicas_filter_pushdown = 1;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_pr_local_pd WHERE a = 5)
WHERE explain LIKE '%Prewhere filter column%';
SELECT * FROM v_pr_local_pd WHERE a = 5;

DROP VIEW v_pr_local_pd;
DROP TABLE t_pr_local_pd;
