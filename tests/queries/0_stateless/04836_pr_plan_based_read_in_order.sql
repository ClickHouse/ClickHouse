-- Tags: no-random-merge-tree-settings

-- Read in order with `ORDER BY` under plan-based parallel replicas (`parallel_replicas_plan_based`).
--
-- The split is lifted through the `SortingStep`, so the sort ships inside the fragment and each replica reads
-- in order; the initiator merges the already sorted streams. Both sides re-derive read-in-order from the same
-- sort description, so they agree on `CoordinationMode::WithOrder` (ASC) or `ReverseOrder` (DESC) - a
-- disagreement would throw "Replica ... decided to read in ... mode, not in ...".
--
-- The correctness half of the test is direction-agnostic: `ORDER BY` results must be identical to
-- non-parallel execution, with LIMIT and with a deep OFFSET. Every ordered query is run with the split off
-- and on, so the two blocks must match line for line - a rewrite that loses one side of the union, or a merge
-- that does not respect the shipped sort description, changes them.

DROP TABLE IF EXISTS t_pr_read_in_order;

CREATE TABLE t_pr_read_in_order (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 128;

INSERT INTO t_pr_read_in_order SELECT number, number % 10 FROM numbers(100000);
OPTIMIZE TABLE t_pr_read_in_order FINAL;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_plan_based = 1;
SET optimize_read_in_order = 1;
-- Pin the manual mode: CI randomizes `automatic_parallel_replicas_mode` to 2, and the cost model may then
-- decide against parallel replicas, so the plan-based split would never engage.
SET automatic_parallel_replicas_mode = 0;

-- The rewritten plan still returns every row exactly once (a = 0..99999). No ORDER BY here on purpose - an
-- aggregate over an ordered subquery would have the sort removed as redundant, so it would not test anything
-- extra.
SELECT '--- every row read exactly once, plan_based = 1 ---';
SELECT count() = 100000, sum(a) = 4999950000 FROM t_pr_read_in_order;

-- ORDER BY ... LIMIT is the shape read-in-order exists to accelerate. Printing the rows checks the ordering
-- itself, not just the row count.
SELECT '--- ORDER BY a LIMIT 5, local ---';
SELECT a, b FROM t_pr_read_in_order ORDER BY a      LIMIT 5 SETTINGS enable_parallel_replicas = 0;
SELECT '--- ORDER BY a LIMIT 5, plan_based = 1 ---';
SELECT a, b FROM t_pr_read_in_order ORDER BY a      LIMIT 5;
SELECT '--- ORDER BY a DESC LIMIT 5, local ---';
SELECT a, b FROM t_pr_read_in_order ORDER BY a DESC LIMIT 5 SETTINGS enable_parallel_replicas = 0;
SELECT '--- ORDER BY a DESC LIMIT 5, plan_based = 1 ---';
SELECT a, b FROM t_pr_read_in_order ORDER BY a DESC LIMIT 5;

-- A deep OFFSET checks the bound shipped with the fragment: a replica may stop at `limit` + `offset` rows,
-- but not at `limit` - the OFFSET applies once, above the merge, not on each replica.
SELECT '--- ORDER BY a, deep OFFSET, local ---';
SELECT a FROM t_pr_read_in_order ORDER BY a      LIMIT 5 OFFSET 49997 SETTINGS enable_parallel_replicas = 0;
SELECT '--- ORDER BY a, deep OFFSET, plan_based = 1 ---';
SELECT a FROM t_pr_read_in_order ORDER BY a      LIMIT 5 OFFSET 49997;
SELECT '--- ORDER BY a DESC, deep OFFSET, local ---';
SELECT a FROM t_pr_read_in_order ORDER BY a DESC LIMIT 5 OFFSET 49997 SETTINGS enable_parallel_replicas = 0;
SELECT '--- ORDER BY a DESC, deep OFFSET, plan_based = 1 ---';
SELECT a FROM t_pr_read_in_order ORDER BY a DESC LIMIT 5 OFFSET 49997;

-- `WITH TIES` extends the LIMIT to every row tying with the last one, so it depends on the full ordered
-- stream rather than on the first N rows. Shipping a per-replica `LimitStep` under a merge could cut the tie
-- group short. b = a % 10, so the first tie group is all 10000 rows with b = 0.
SELECT '--- WITH TIES, local ---';
SELECT count(), sum(a) FROM (SELECT a FROM t_pr_read_in_order ORDER BY b LIMIT 3 WITH TIES) SETTINGS enable_parallel_replicas = 0;
SELECT '--- WITH TIES, plan_based = 1 ---';
SELECT count(), sum(a) FROM (SELECT a FROM t_pr_read_in_order ORDER BY b LIMIT 3 WITH TIES);

-- `LIMIT BY` picks N rows per key from the ordered stream, so it is order-sensitive in a different way: the
-- per-key selection happens on the initiator, above the merge of the per-replica streams.
SELECT '--- LIMIT BY, local ---';
SELECT a FROM t_pr_read_in_order ORDER BY a LIMIT 2 BY b LIMIT 6 SETTINGS enable_parallel_replicas = 0;
SELECT '--- LIMIT BY, plan_based = 1 ---';
SELECT a FROM t_pr_read_in_order ORDER BY a LIMIT 2 BY b LIMIT 6;

-- Ordering must survive a filter too (the filter is pushed into the shipped fragment).
SELECT '--- WHERE + ORDER BY a, local ---';
SELECT a FROM t_pr_read_in_order WHERE b = 3 ORDER BY a      LIMIT 5 OFFSET 10 SETTINGS enable_parallel_replicas = 0;
SELECT '--- WHERE + ORDER BY a, plan_based = 1 ---';
SELECT a FROM t_pr_read_in_order WHERE b = 3 ORDER BY a      LIMIT 5 OFFSET 10;
SELECT '--- WHERE + ORDER BY a DESC, local ---';
SELECT a FROM t_pr_read_in_order WHERE b = 3 ORDER BY a DESC LIMIT 5 OFFSET 10 SETTINGS enable_parallel_replicas = 0;
SELECT '--- WHERE + ORDER BY a DESC, plan_based = 1 ---';
SELECT a FROM t_pr_read_in_order WHERE b = 3 ORDER BY a DESC LIMIT 5 OFFSET 10;

-- The split engaged: the read became a UNION of a local read and a remote parallel-replicas read.
SELECT '--- explain: has_union, has_remote_read, has_local_read ---';
SELECT
    countIf(explain LIKE '%Union%') > 0 AS has_union,
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%ReadFromMergeTree%') > 0 AS has_local_read
FROM (EXPLAIN pretty = 0, description = 0 SELECT a FROM t_pr_read_in_order ORDER BY a LIMIT 5);

-- The local read is in order, in both directions. Without the sort inside the fragment the read would fall
-- back to `ReadType: Default` and the coordinator to `CoordinationMode::Default`.
SELECT '--- explain: ReadType InOrder for ORDER BY a ---';
SELECT countIf(explain LIKE '%ReadType: InOrder%') > 0
FROM (EXPLAIN pretty = 0, description = 0 SELECT a FROM t_pr_read_in_order ORDER BY a LIMIT 5);
SELECT '--- explain: ReadType InReverseOrder for ORDER BY a DESC ---';
SELECT countIf(explain LIKE '%ReadType: InReverseOrder%') > 0
FROM (EXPLAIN pretty = 0, description = 0 SELECT a FROM t_pr_read_in_order ORDER BY a DESC LIMIT 5);

-- ... and the pipeline uses the in-order read pool rather than the default one, the same as classic parallel
-- replicas does (see 04073_parallel_replicas_in_order_splits).
SELECT '--- explain pipeline: in-order read pool for ORDER BY a ---';
SELECT countIf(explain LIKE '%ReadPoolParallelReplicasInOrder%') > 0
FROM (EXPLAIN PIPELINE SELECT a FROM t_pr_read_in_order ORDER BY a LIMIT 5);
SELECT '--- explain pipeline: in-order read pool for ORDER BY a DESC ---';
SELECT countIf(explain LIKE '%ReadPoolParallelReplicasInOrder%') > 0
FROM (EXPLAIN PIPELINE SELECT a FROM t_pr_read_in_order ORDER BY a DESC LIMIT 5);

-- A top-N is restated as a `LimitStep` inside the fragment, because `SortingStep::serialize` drops the sort's
-- own limit. That row cut must not be shipped when `exact_rows_before_limit` is on: it would truncate a
-- replica's stream before `rows_before_limit_at_least` is counted. So the "local top-N" step is expected only
-- when the statistic is not requested.
SELECT '--- explain: local top-N shipped, exact_rows_before_limit = 0 ---';
SELECT countIf(explain LIKE '%local top-N%') > 0
FROM (EXPLAIN pretty = 0 SELECT a FROM t_pr_read_in_order ORDER BY a LIMIT 5 SETTINGS exact_rows_before_limit = 0);
SELECT '--- explain: local top-N shipped, exact_rows_before_limit = 1 ---';
SELECT countIf(explain LIKE '%local top-N%') > 0
FROM (EXPLAIN pretty = 0 SELECT a FROM t_pr_read_in_order ORDER BY a LIMIT 5 SETTINGS exact_rows_before_limit = 1);

-- The rows are still correct with the statistic requested, in both directions. (The value of
-- `rows_before_limit_at_least` itself is not asserted: it is still inexact under
-- `parallel_replicas_local_plan = 1`, the same as classic parallel replicas - see the FIXME in
-- `applyParallelReplicas.cpp` and https://github.com/ClickHouse/ClickHouse/issues/114723.)
SELECT '--- exact_rows_before_limit = 1, ORDER BY a, local ---';
SELECT a FROM t_pr_read_in_order ORDER BY a      LIMIT 5 SETTINGS exact_rows_before_limit = 1, enable_parallel_replicas = 0;
SELECT '--- exact_rows_before_limit = 1, ORDER BY a, plan_based = 1 ---';
SELECT a FROM t_pr_read_in_order ORDER BY a      LIMIT 5 SETTINGS exact_rows_before_limit = 1;
SELECT '--- exact_rows_before_limit = 1, ORDER BY a DESC, local ---';
SELECT a FROM t_pr_read_in_order ORDER BY a DESC LIMIT 5 SETTINGS exact_rows_before_limit = 1, enable_parallel_replicas = 0;
SELECT '--- exact_rows_before_limit = 1, ORDER BY a DESC, plan_based = 1 ---';
SELECT a FROM t_pr_read_in_order ORDER BY a DESC LIMIT 5 SETTINGS exact_rows_before_limit = 1;

DROP TABLE t_pr_read_in_order;
