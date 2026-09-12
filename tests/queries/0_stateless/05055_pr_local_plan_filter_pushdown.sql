-- A condition sitting above the fragment parallel replicas execute goes into the initiator's local
-- copy of that fragment whether or not the replicas also get it: it changes which rows this replica
-- reads, which is nobody else's business. What it may not do is decide *how* the fragment reads.
-- Fixing a sort key column is what would: with `tenant` pinned to one value a sort on the rest of the
-- key can be answered by reading in order, and then the initiator announces `WithOrder` to the shared
-- coordinator while the replicas, which never saw the condition, announce `Default`.
--
-- So the fragment derives ordering only from the columns its own filters fix - the ones the replicas
-- fix too, from their copy of the same fragment - and a condition arriving from outside prunes without
-- ordering. `parallel_replicas_filter_pushdown` puts it in the replicas' query as well, and then the
-- ordering is theirs to derive too and nothing is held back.

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
-- `parallel_replicas_filter_pushdown` puts the condition in the replicas' query by rewriting it. Pin
-- the two settings that decide whether that rewrite reaches them.
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET serialize_query_plan = 0;
-- An ordered read is what the fragment must not reach for on a condition of its own, so ask for one.
SET optimize_read_in_order = 1;

SELECT 'equality: prunes the local read, does not order it';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_pr_local_pd WHERE tenant = 5 LIMIT 5)
WHERE explain LIKE '%Prewhere filter column%' OR explain LIKE '%Read type%';
SELECT count() FROM v_pr_local_pd WHERE tenant = 5;

SELECT 'equality, setting enabled: orders it as well';
SET parallel_replicas_filter_pushdown = 1;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_pr_local_pd WHERE tenant = 5 LIMIT 5)
WHERE explain LIKE '%Prewhere filter column%' OR explain LIKE '%Read type%';
SELECT count() FROM v_pr_local_pd WHERE tenant = 5;
SET parallel_replicas_filter_pushdown = 0;

SELECT 'the same equality inside the fragment orders it';
-- Nothing is withheld here: the replicas run this fragment too, so they fix `tenant` as well.
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_own_pr_local_pd LIMIT 5)
WHERE explain LIKE '%Prewhere filter column%' OR explain LIKE '%Read type%';
SELECT count() FROM v_own_pr_local_pd;

SELECT 'comparison: nothing is fixed, so nothing is withheld';
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
