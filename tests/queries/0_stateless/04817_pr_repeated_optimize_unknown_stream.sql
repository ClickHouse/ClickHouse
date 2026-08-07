-- Tags: no-parallel-replicas
-- A query plan can be optimized more than once, and `ReadFromMerge` does exactly that for its child
-- plans. The parallel replicas rewrite was not idempotent: after the first run the inlined local plan
-- already carries a coordinated read, and a second run distributed it again, so the query got two
-- coordinators for one read. Only the first ever registered a stream, and a follower announcing to
-- the second was rejected and then aborted the initiator with
-- `LOGICAL_ERROR: Got read request from replica N for unknown stream <table>`.
-- The witnesses below therefore killed the server before the fix.
-- The controls read the same data by paths that never had the duplicate, so they pass with and
-- without the fix on purpose: they fail only if the guard skips the rewrite too broadly and costs a
-- first-time plan its distribution or its index analysis.
-- See issue #110518.

DROP TABLE IF EXISTS t_pr_ro;
DROP TABLE IF EXISTS t2_pr_ro;
DROP VIEW IF EXISTS v_pr_ro;

CREATE TABLE t_pr_ro (timestamp DateTime, value UInt32)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY timestamp;

CREATE TABLE t2_pr_ro (timestamp DateTime, value UInt32)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY timestamp;

CREATE VIEW v_pr_ro AS SELECT * FROM t_pr_ro;

INSERT INTO t_pr_ro
SELECT toDateTime('2026-06-01 00:00:00') + number, number FROM numbers(100000);

INSERT INTO t2_pr_ro
SELECT toDateTime('2026-06-01 00:00:00') + number, number FROM numbers(1000);

SET enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 1;

-- Witness: one merged table. Aborted the server before the fix.
SELECT 'merge one table';
SELECT sum(value) FROM merge(currentDatabase(), '^t_pr_ro$');

-- Witness: two merged tables. The duplicate was created once per merged table, so a fix that only
-- handled a single branch would still abort here.
SELECT 'merge two tables';
SELECT sum(value) FROM merge(currentDatabase(), '^(t_pr_ro|t2_pr_ro)$');

-- Witness: a filter pushed into the child read.
SELECT 'merge with filter';
SELECT sum(value) FROM merge(currentDatabase(), '^t_pr_ro$') WHERE value > 5;

-- Witness: a JOIN over the merged table. This one reaches `ReadFromMerge::addFilter`, which optimizes
-- the child plan from a second call site, so it pins that the guard covers the transform rather than
-- one caller of it.
SELECT 'merge joined';
SELECT sum(a.value) FROM merge(currentDatabase(), '^t_pr_ro$') a
JOIN t2_pr_ro b ON a.value = b.value
WHERE a.value > 5;

-- Witness: the same JOIN with the merged table on the right.
SELECT 'merge joined reversed';
SELECT sum(a.value) FROM t2_pr_ro b
JOIN merge(currentDatabase(), '^t_pr_ro$') a ON a.value = b.value
WHERE a.value > 5;

-- Witness: the child read's index analysis must survive the guard. `force_index_by_date` and
-- `force_primary_key` throw if the partition and primary key are not used, so a lost analysis fails
-- this arm.
SELECT 'merge with forced index';
SELECT count() FROM merge(currentDatabase(), '^t_pr_ro$')
WHERE timestamp >= toDateTime('2026-06-02 00:00:00')
SETTINGS force_index_by_date = 1, force_primary_key = 1;

-- Witness: index analysis is reported for the child read, independent of granule counts.
SELECT 'merge index analysis';
SELECT countIf(explain LIKE '%Min-Max%') > 0, countIf(explain LIKE '%Partition%') > 0,
       countIf(explain LIKE '%PrimaryKey%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(value) FROM merge(currentDatabase(), '^t_pr_ro$')
      WHERE timestamp >= toDateTime('2026-06-02 00:00:00'));

-- Negative: a predicate that uses neither key must still be rejected, so the arm above passes because
-- the index is used and not because forcing it became a no-op.
SELECT 'merge forced index not used';
SELECT count() FROM merge(currentDatabase(), '^t_pr_ro$')
WHERE value = 1
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

-- Control: reading the table directly is a first-time plan, so the guard must not skip its
-- distribution. Results alone cannot show that, because parallel replicas is an optimization and a
-- skipped one still returns the right answer, so assert the distributed read is in the plan.
SELECT 'base table';
SELECT sum(value) FROM t_pr_ro;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN SELECT sum(value) FROM t_pr_ro);

-- Control: reading through a plain view never had a duplicate either.
SELECT 'view';
SELECT sum(value) FROM v_pr_ro;

-- Control: the classic (not plan-based) parallel replicas path, which the guard never reaches.
SELECT 'merge without plan based';
SELECT sum(value) FROM merge(currentDatabase(), '^t_pr_ro$')
SETTINGS parallel_replicas_plan_based = 0;

-- Control: without a local plan there is no inlined coordinated read for a second pass to
-- redistribute.
SELECT 'merge without local plan';
SELECT sum(value) FROM merge(currentDatabase(), '^t_pr_ro$')
SETTINGS parallel_replicas_local_plan = 0;

-- Control: no parallel replicas at all.
SELECT 'merge without parallel replicas';
SELECT sum(value) FROM merge(currentDatabase(), '^t_pr_ro$')
SETTINGS enable_parallel_replicas = 0;

DROP VIEW v_pr_ro;
DROP TABLE t2_pr_ro;
DROP TABLE t_pr_ro;
