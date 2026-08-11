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
-- and on, so the two blocks must match line for line - a broken per-replica range assignment would drop or
-- duplicate rows, and a broken merge would reorder them.

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
SET optimize_read_in_order = 1;
-- Pin the manual mode: CI randomizes `automatic_parallel_replicas_mode` to 2, and the cost model may then
-- decide against parallel replicas, so the plan-based split would never engage.
SET automatic_parallel_replicas_mode = 0;

-- Coverage guard: every row read exactly once (a = 0..99999). No ORDER BY here on purpose - an aggregate
-- over an ordered subquery would have the sort removed as redundant, so it would not test anything extra.
SELECT 'count+sum', count() = 100000, sum(a) = 4999950000 FROM t_pr_read_in_order SETTINGS parallel_replicas_plan_based = 1;

-- ORDER BY ... LIMIT is the shape read-in-order exists to accelerate. Printing the rows checks the ordering
-- itself, not just the row count.
SELECT 'asc  limit', a, b FROM t_pr_read_in_order ORDER BY a      LIMIT 5 SETTINGS parallel_replicas_plan_based = 0;
SELECT 'asc  limit', a, b FROM t_pr_read_in_order ORDER BY a      LIMIT 5 SETTINGS parallel_replicas_plan_based = 1;
SELECT 'desc limit', a, b FROM t_pr_read_in_order ORDER BY a DESC LIMIT 5 SETTINGS parallel_replicas_plan_based = 0;
SELECT 'desc limit', a, b FROM t_pr_read_in_order ORDER BY a DESC LIMIT 5 SETTINGS parallel_replicas_plan_based = 1;

-- An OFFSET deep into the data: a per-replica range assignment that silently dropped or duplicated a range
-- shifts these rows even when the first page still looks right.
SELECT 'asc  offset', a FROM t_pr_read_in_order ORDER BY a      LIMIT 5 OFFSET 49997 SETTINGS parallel_replicas_plan_based = 0;
SELECT 'asc  offset', a FROM t_pr_read_in_order ORDER BY a      LIMIT 5 OFFSET 49997 SETTINGS parallel_replicas_plan_based = 1;
SELECT 'desc offset', a FROM t_pr_read_in_order ORDER BY a DESC LIMIT 5 OFFSET 49997 SETTINGS parallel_replicas_plan_based = 0;
SELECT 'desc offset', a FROM t_pr_read_in_order ORDER BY a DESC LIMIT 5 OFFSET 49997 SETTINGS parallel_replicas_plan_based = 1;

-- Ordering must survive a filter too (the filter is pushed into the shipped fragment).
SELECT 'asc  where', a FROM t_pr_read_in_order WHERE b = 3 ORDER BY a      LIMIT 5 OFFSET 10 SETTINGS parallel_replicas_plan_based = 0;
SELECT 'asc  where', a FROM t_pr_read_in_order WHERE b = 3 ORDER BY a      LIMIT 5 OFFSET 10 SETTINGS parallel_replicas_plan_based = 1;
SELECT 'desc where', a FROM t_pr_read_in_order WHERE b = 3 ORDER BY a DESC LIMIT 5 OFFSET 10 SETTINGS parallel_replicas_plan_based = 0;
SELECT 'desc where', a FROM t_pr_read_in_order WHERE b = 3 ORDER BY a DESC LIMIT 5 OFFSET 10 SETTINGS parallel_replicas_plan_based = 1;

SET parallel_replicas_plan_based = 1;

-- The split engaged: the read became a UNION of a local read and a remote parallel-replicas read.
SELECT
    'split',
    countIf(explain LIKE '%Union%') > 0 AS has_union,
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%ReadFromMergeTree%') > 0 AS has_local_read
FROM (EXPLAIN pretty = 0, description = 0 SELECT a FROM t_pr_read_in_order ORDER BY a LIMIT 5);

-- The local read is in order, in both directions. Without the sort inside the fragment the read would fall
-- back to `ReadType: Default` and the coordinator to `CoordinationMode::Default`.
SELECT 'read_type asc',  countIf(explain LIKE '%ReadType: InOrder%') > 0
FROM (EXPLAIN pretty = 0, description = 0 SELECT a FROM t_pr_read_in_order ORDER BY a LIMIT 5);
SELECT 'read_type desc', countIf(explain LIKE '%ReadType: InReverseOrder%') > 0
FROM (EXPLAIN pretty = 0, description = 0 SELECT a FROM t_pr_read_in_order ORDER BY a DESC LIMIT 5);

-- ... and the pipeline uses the in-order read pool rather than the default one, the same as classic parallel
-- replicas does (see 04073_parallel_replicas_in_order_splits).
SELECT 'in_order_pool asc',  countIf(explain LIKE '%ReadPoolParallelReplicasInOrder%') > 0
FROM (EXPLAIN PIPELINE SELECT a FROM t_pr_read_in_order ORDER BY a LIMIT 5);
SELECT 'in_order_pool desc', countIf(explain LIKE '%ReadPoolParallelReplicasInOrder%') > 0
FROM (EXPLAIN PIPELINE SELECT a FROM t_pr_read_in_order ORDER BY a DESC LIMIT 5);

DROP TABLE t_pr_read_in_order;
