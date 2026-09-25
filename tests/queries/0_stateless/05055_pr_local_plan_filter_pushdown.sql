-- A condition sitting above the fragment parallel replicas execute goes into the initiator's local
-- copy of that fragment and is spliced into the query the replicas run, so both sides prune by it and
-- both may order by it: a `tenant` pinned to one value lets a sort on the rest of the key be answered
-- by reading in order, and the initiator and the replicas agree because they hold the same condition.
--
-- Where the splice does not reach - a fragment that unions or joins, one the rewrite refuses, a
-- runtime filter it cannot express - only this replica has the condition, and then it may prune by it
-- but not order by it, or the initiator would announce `WithOrder` to the shared coordinator while the
-- replicas announce `Default`. Those shapes are covered by 05161, 05182 and 05183.

DROP TABLE IF EXISTS t_pr_local_pd;
DROP VIEW IF EXISTS v_pr_local_pd;
DROP VIEW IF EXISTS v_own_pr_local_pd;

CREATE TABLE t_pr_local_pd (tenant UInt64, ts UInt64, name String, flag UInt8)
    ENGINE = MergeTree ORDER BY (tenant, ts) SETTINGS index_granularity = 128;
INSERT INTO t_pr_local_pd SELECT number % 100, number, toString(number), number % 2 FROM numbers(10000);

-- The view's own `ORDER BY` puts the sort inside the fragment, so an equality on the sort key prefix
-- is what would make its read go in order.
CREATE VIEW v_pr_local_pd AS SELECT * FROM t_pr_local_pd ORDER BY ts;
-- The same sort with the equality already inside the fragment - the replicas execute this one too.
CREATE VIEW v_own_pr_local_pd AS SELECT * FROM t_pr_local_pd WHERE tenant = 5 ORDER BY ts;

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
-- The plans below are about the condition reaching the read as a `PREWHERE`, so pin the two
-- optimizations that fold it in.
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;
-- The rewrite that puts the condition in the replicas' query answers to these two, so pin them.
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET serialize_query_plan = 0;
-- An ordered read is what the fragment must not reach for on a condition of its own, so ask for one.
SET optimize_read_in_order = 1;

SELECT 'equality: prunes the read and orders it, because the replicas have it too';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_pr_local_pd WHERE tenant = 5 LIMIT 5)
WHERE explain LIKE '%Prewhere filter column%' OR explain LIKE '%Read type%';
SELECT count() FROM v_pr_local_pd WHERE tenant = 5;

SELECT 'the same equality written inside the fragment orders it too';
-- The replicas run this fragment as written, so they fix `tenant` from their own copy of it.
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_own_pr_local_pd LIMIT 5)
WHERE explain LIKE '%Prewhere filter column%' OR explain LIKE '%Read type%';
SELECT count() FROM v_own_pr_local_pd;

SELECT 'comparison: nothing is fixed, so there is no ordering to derive';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_pr_local_pd WHERE tenant > 90 LIMIT 5)
WHERE explain LIKE '%Prewhere filter column%' OR explain LIKE '%Read type%';
SELECT count() FROM v_pr_local_pd WHERE tenant > 90;

SELECT 'equality off the sorting key';
-- `name` is not in the sorting key, so nothing it fixes could order this read anyway.
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_pr_local_pd WHERE name = '5' LIMIT 5)
WHERE explain LIKE '%Prewhere filter column%' OR explain LIKE '%Read type%';
SELECT count() FROM v_pr_local_pd WHERE name = '5';

SELECT 'bare boolean';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_pr_local_pd WHERE flag LIMIT 5)
WHERE explain LIKE '%Prewhere filter column%' OR explain LIKE '%Read type%';
SELECT count() FROM v_pr_local_pd WHERE flag;

DROP VIEW v_own_pr_local_pd;
DROP VIEW v_pr_local_pd;
DROP TABLE t_pr_local_pd;
