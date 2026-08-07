-- Tags: no-parallel-replicas
-- A query plan can be optimized more than once, and `ReadFromMerge` does exactly that for its child
-- plans. The parallel replicas rewrite was not idempotent, so a second run distributed an already
-- coordinated read again: the query got two coordinators for one read, which aborted the server.
-- The witnesses below therefore killed the server before the fix, except the two that assert a plan
-- shape instead: one that the merged child read is still distributed after the guard, and one that
-- ordering is declined for a read shipped without a local plan.
-- The controls read the same data by paths that never had the duplicate, so they pass either way and
-- fail only if the guard skips the rewrite too broadly. See issue #110518.

DROP TABLE IF EXISTS t_pr_ro;
DROP TABLE IF EXISTS t2_pr_ro;
DROP VIEW IF EXISTS v_pr_ro;
DROP VIEW IF EXISTS v_ordered_pr_ro;

CREATE TABLE t_pr_ro (timestamp DateTime, value UInt32)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY timestamp;

CREATE TABLE t2_pr_ro (timestamp DateTime, value UInt32)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY timestamp;

CREATE VIEW v_pr_ro AS SELECT * FROM t_pr_ro;

CREATE VIEW v_ordered_pr_ro AS SELECT * FROM t_pr_ro ORDER BY timestamp LIMIT 50000;

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

-- Witness: an ordered read through the merged table. Read-in-order runs after the rewrite has already
-- shipped the fragment, and it can only reach the local read, so ordering a coordinated child would
-- leave the two sides in different coordination modes and abort with the same unknown-stream error.
-- `optimize_read_in_order` is randomized by the test runner and is what enables the optimization,
-- so pin it here or this arm passes for the wrong reason.
SELECT 'merge ordered';
SELECT value FROM merge(currentDatabase(), '^t_pr_ro$') ORDER BY timestamp LIMIT 3
SETTINGS optimize_read_in_order = 1;

-- Witness: the sort arrives from inside the view, so it sits within the child plan rather than above
-- `ReadFromMerge`. Ordering is then decided by the union branch of read-in-order, not by
-- `ReadFromMerge::requestReadingInOrder`, so this pins the decline on that second entry point.
SELECT 'merge ordered view';
SELECT sum(value) FROM merge(currentDatabase(), '^v_ordered_pr_ro$')
SETTINGS optimize_read_in_order = 1;

-- Witness: the arm above asserts only a sum, which a purely local read returns too, so it would go
-- green if the plan for this shape ever stopped being distributed. Assert the shipped fragment is in
-- the plan, with the same query at `enable_parallel_replicas = 0` as the control that the assertion
-- discriminates rather than matching everything.
SELECT 'merge ordered view plan shape';
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN SELECT sum(value) FROM merge(currentDatabase(), '^v_ordered_pr_ro$')
      SETTINGS optimize_read_in_order = 1);
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN SELECT sum(value) FROM merge(currentDatabase(), '^v_ordered_pr_ro$')
      SETTINGS optimize_read_in_order = 1, enable_parallel_replicas = 0);

-- Control: the same ordered read with read-in-order off, the configuration that already worked.
SELECT 'merge ordered no read in order';
SELECT value FROM merge(currentDatabase(), '^t_pr_ro$') ORDER BY timestamp LIMIT 3
SETTINGS optimize_read_in_order = 0;

-- Control: index analysis is reported for the child read, independent of granule counts. It runs
-- through EXPLAIN, which never builds a pipeline, so it is unaffected by the duplicate and passes
-- either way.
SELECT 'merge index analysis';
SELECT countIf(explain LIKE '%Min-Max%') > 0, countIf(explain LIKE '%Partition%') > 0,
       countIf(explain LIKE '%PrimaryKey%') > 0
FROM (EXPLAIN indexes = 1 SELECT sum(value) FROM merge(currentDatabase(), '^t_pr_ro$')
      WHERE timestamp >= toDateTime('2026-06-02 00:00:00'));

-- Witness: the merged child read must STILL be distributed after the guard. A result alone cannot
-- show that, since parallel replicas is an optimization and a skipped one returns the same number,
-- so assert the plan shape for the `merge` child directly. The guard must fire only on the second
-- rewrite of this plan, never on the first.
SELECT 'merge plan shape';
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0,
       countIf(explain LIKE '%ReadFromMerge%') > 0
FROM (EXPLAIN SELECT sum(value) FROM merge(currentDatabase(), '^t_pr_ro$'));

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

-- Witness: without a local plan the child read is the shipped fragment alone, with no local read to
-- recognise it by, so this pins that ordering is declined for that shape too. Rows are not an oracle
-- here, since ORDER BY sorts either way, so assert the sort was not converted to a partially sorted
-- one: `Prefix sort description` is printed only for a sort with a non-empty prefix. Before the fix
-- this arm read 1, and the query it explains returned the wrong three rows in 6 of 40 runs.
SELECT 'merge ordered without local plan';
SELECT countIf(explain LIKE '%Prefix sort description%')
FROM (EXPLAIN actions = 1 SELECT value FROM merge(currentDatabase(), '^t_pr_ro$') ORDER BY timestamp LIMIT 3
      SETTINGS optimize_read_in_order = 1, parallel_replicas_local_plan = 0);

-- Control: no parallel replicas at all.
SELECT 'merge without parallel replicas';
SELECT sum(value) FROM merge(currentDatabase(), '^t_pr_ro$')
SETTINGS enable_parallel_replicas = 0;

DROP VIEW v_ordered_pr_ro;
DROP VIEW v_pr_ro;
DROP TABLE t2_pr_ro;
DROP TABLE t_pr_ro;
