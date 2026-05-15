-- Tags: no-parallel, no-parallel-replicas, no-shared-merge-tree, no-replicated-database
-- ^ The test references a specific `parallel_replicas` cluster definition and recreates the same
--   database table in each iteration; the replicated / parallel-replica test variants override
--   clusters and would not exercise the regressed code path.
--
-- Regression test for issue #104863:
--   SELECT ... FROM loop(remote(...)) GROUP BY ...
-- crashed with `LOGICAL_ERROR: 'Chunk info was not set for chunk in MergingAggregatedTransform.'`
-- when parallel replicas were enabled, and silently dropped the outer aggregation entirely
-- (returned duplicated rows) when they were not.
--
-- The bug came from `StorageLoop::getQueryProcessingStage` delegating to its inner storage —
-- for a wrapped Distributed / remote table it would advertise `WithMergeableState`, which made
-- the outer planner add `MergingAggregatedStep`. But `LoopSource` always materialises the
-- inner select via `InterpreterSelectQuery*` with `QueryProcessingStage::Complete`, so the
-- chunks it emits carry no `AggregatedChunkInfo`, tripping the logical error.
--
-- After the fix `StorageLoop` reports `FetchColumns`, matching what `LoopSource` actually
-- produces. The outer planner now adds a regular `AggregatingStep` instead of
-- `MergingAggregatedStep`, so the aggregation is correctly applied and the assertion no
-- longer fires.

DROP TABLE IF EXISTS t_loop_104863;

CREATE TABLE t_loop_104863 (c0 Int32) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_loop_104863 SELECT number FROM numbers(5);

-- Structural fix #1: with `enable_parallel_replicas`, the plan must use `Aggregating`,
-- not `MergingAggregated`. The presence of `MergingAggregated` is what caused the crash:
-- it asserts every input chunk has an `AggregatedChunkInfo`, but `LoopSource` emits raw
-- chunks. Print one row per pipeline-step name: `<MergingAggregated rows> <Aggregating rows>`
-- (steps appear as their own line in the `EXPLAIN PLAN` output).
SELECT countIf(trimBoth(explain) = 'MergingAggregated') AS merging_aggregated_steps,
       countIf(trimBoth(explain) = 'Aggregating') AS aggregating_steps
FROM
(
    EXPLAIN PLAN
    SELECT c0
    FROM loop(remote('127.0.0.1', currentDatabase(), 't_loop_104863')) AS tx
    GROUP BY c0
    SETTINGS enable_parallel_replicas = 1,
             cluster_for_parallel_replicas = 'parallel_replicas',
             max_parallel_replicas = 3,
             parallel_replicas_for_non_replicated_merge_tree = 1
);

-- Structural fix #2: without parallel replicas the planner used to add no aggregation step
-- at all (silently produced duplicated rows). After the fix it must add a proper
-- `Aggregating` step.
SELECT countIf(trimBoth(explain) = 'MergingAggregated') AS merging_aggregated_steps,
       countIf(trimBoth(explain) = 'Aggregating') AS aggregating_steps
FROM
(
    EXPLAIN PLAN
    SELECT c0
    FROM loop(remote('127.0.0.1', currentDatabase(), 't_loop_104863')) AS tx
    GROUP BY c0
);

-- Non-aggregating sanity check: `loop(remote(...))` still produces rows and honours
-- a pushed-down `LIMIT` (this exercises the `trivial_limit` path in `LoopSource`).
SELECT count() FROM
(
    SELECT c0
    FROM loop(remote('127.0.0.1', currentDatabase(), 't_loop_104863')) AS tx
    LIMIT 25
);

DROP TABLE t_loop_104863;
