-- A `Merge` table builds the plans of its underlying tables lazily, while the outer plan is
-- already being executed. Plan-based parallel replicas must not distribute such a child plan:
-- the shipped fragment used to lose the filters pushed down into it (silently returning wrong
-- results) and, when the filter referenced a subquery set whose plan the outer
-- `addStepsToBuildSets` had already moved out, serializing it threw
-- `Cannot serialize FutureSetFromSubquery with no query plan`.

DROP TABLE IF EXISTS t_merge_pr_local;
DROP TABLE IF EXISTS t_merge_pr_dist;

CREATE TABLE t_merge_pr_local (name String) ENGINE = MergeTree ORDER BY name;
INSERT INTO t_merge_pr_local SELECT toString(number) FROM numbers(100);

CREATE TABLE t_merge_pr_dist AS t_merge_pr_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_merge_pr_local);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_local_plan = 0;

SELECT 'filters are honored';
-- Each of these used to return 0.
SELECT count() FROM merge(currentDatabase(), 't_merge_pr_local') WHERE name = '1';
SELECT count() FROM merge(currentDatabase(), 't_merge_pr_local') WHERE name IN ('1', '2', '3');
SELECT count() FROM merge(currentDatabase(), 't_merge_pr_local') WHERE length(name) > 1;
SELECT count() FROM merge(currentDatabase(), 't_merge_pr_local');

SELECT 'IN (subquery)';
-- Used to throw LOGICAL_ERROR `Cannot serialize FutureSetFromSubquery with no query plan`.
SELECT count() FROM merge(currentDatabase(), 't_merge_pr_local')
WHERE name IN (SELECT name FROM t_merge_pr_local);

SELECT 'GLOBAL IN (subquery over Distributed)';
-- The shape the AST fuzzer hit.
SELECT count() FROM merge(currentDatabase(), 't_merge_pr_local')
WHERE name GLOBAL IN (SELECT name FROM t_merge_pr_dist);

SELECT 'the child read of a Merge table is not distributed';
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') AS child_read_distributed
FROM (EXPLAIN description = 0 SELECT count() FROM merge(currentDatabase(), 't_merge_pr_local') WHERE name = '1');

SELECT 'a plain table is still distributed';
-- Regression guard against over-disabling parallel replicas.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS plain_read_distributed
FROM (EXPLAIN description = 0 SELECT count() FROM t_merge_pr_local WHERE name = '1');

SELECT 'same answers with parallel_replicas_local_plan = 1';
SET parallel_replicas_local_plan = 1;
SELECT count() FROM merge(currentDatabase(), 't_merge_pr_local') WHERE name = '1';
SELECT count() FROM merge(currentDatabase(), 't_merge_pr_local')
WHERE name IN (SELECT name FROM t_merge_pr_local);
SELECT count() FROM merge(currentDatabase(), 't_merge_pr_local')
WHERE name GLOBAL IN (SELECT name FROM t_merge_pr_dist);

-- With a local plan the distributed read is wrapped in `ReadFromLocalReplica` /
-- `ReadFromRemoteParallelReplicas` instead of `ReadFromParallelReplicas`, so the check above
-- would not notice a regression in this mode.
SELECT 'the child read of a Merge table is not distributed with a local plan';
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%'
            OR explain LIKE '%ReadFromLocalReplica%'
            OR explain LIKE '%ReadFromRemoteParallelReplicas%') AS child_read_distributed
FROM (EXPLAIN description = 0 SELECT count() FROM merge(currentDatabase(), 't_merge_pr_local') WHERE name = '1');

SELECT 'a plain table is still distributed with a local plan';
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%'
            OR explain LIKE '%ReadFromLocalReplica%'
            OR explain LIKE '%ReadFromRemoteParallelReplicas%') > 0 AS plain_read_distributed
FROM (EXPLAIN description = 0 SELECT count() FROM t_merge_pr_local WHERE name = '1');

DROP TABLE t_merge_pr_dist;
DROP TABLE t_merge_pr_local;
