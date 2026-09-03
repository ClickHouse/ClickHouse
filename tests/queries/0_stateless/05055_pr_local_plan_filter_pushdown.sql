-- A condition above the part of the query parallel replicas execute may enter the initiator's local
-- plan on its own only if it cannot decide how that plan reads. Fixing a column is what decides it:
-- with `tenant` pinned to one value a sort or an aggregation on the rest of the sort key reads in
-- order, and the initiator would announce a coordination mode the replicas do not use (see 05057).
-- So an equality waits for `parallel_replicas_filter_pushdown`, which puts it in the replicas' query
-- too, while everything else - a comparison, a bare boolean, a join runtime filter (see 05056) - goes
-- in regardless.
--
-- The test is drawn along the same coarse line the code uses: any equality waits, whatever column it
-- names and whether or not the sorting key mentions it. `a = 5` below happens to be on the sorting
-- key, but `b = '5'` would be refused just the same.

DROP TABLE IF EXISTS t_pr_local_pd;
DROP VIEW IF EXISTS v_pr_local_pd;

CREATE TABLE t_pr_local_pd (a UInt32, b String, flag UInt8) ENGINE = MergeTree ORDER BY a;
-- Read the outer query over the view, so that the filter starts above the parallel replicas read.
CREATE VIEW v_pr_local_pd AS SELECT * FROM t_pr_local_pd;
INSERT INTO t_pr_local_pd SELECT number, toString(number), number % 2 FROM numbers(1000);

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
-- `parallel_replicas_filter_pushdown` puts the equality in the replicas' query by rewriting it, and
-- only lets it into the local plan when that rewrite reaches them. Pin the two settings that decide
-- whether it does; 05057 covers what happens when it does not.
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET serialize_query_plan = 0;

SELECT 'equality, default: not in the local plan';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_pr_local_pd WHERE a = 5)
WHERE explain LIKE '%Prewhere filter column%';
SELECT * FROM v_pr_local_pd WHERE a = 5;

SELECT 'equality, setting enabled: in the local plan';
SET parallel_replicas_filter_pushdown = 1;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_pr_local_pd WHERE a = 5)
WHERE explain LIKE '%Prewhere filter column%';
SELECT * FROM v_pr_local_pd WHERE a = 5;

SET parallel_replicas_filter_pushdown = 0;

SELECT 'comparison, default: in the local plan';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_pr_local_pd WHERE a > 995)
WHERE explain LIKE '%Prewhere filter column%';
SELECT count() FROM v_pr_local_pd WHERE a > 995;

SELECT 'equality off the sorting key, default: not in the local plan either';
-- `b` is not in the sorting key, so this one could safely go in. The gate does not look at which
-- column an equality names, so it waits for the setting like any other equality.
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_pr_local_pd WHERE b = '5')
WHERE explain LIKE '%Prewhere filter column%';
SELECT * FROM v_pr_local_pd WHERE b = '5';

SELECT 'bare boolean, default: in the local plan';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_pr_local_pd WHERE flag)
WHERE explain LIKE '%Prewhere filter column%';
SELECT count() FROM v_pr_local_pd WHERE flag;

DROP VIEW v_pr_local_pd;
DROP TABLE t_pr_local_pd;
