-- Regression test for issue #104863. `SELECT ... FROM loop(remote(...)) GROUP BY ...`
-- used to raise a `LOGICAL_ERROR` exception "Chunk info was not set for chunk in
-- `MergingAggregatedTransform`" with `enable_parallel_replicas = 1`, and silently
-- dropped the outer aggregation (returning duplicated rows) without it. Both
-- manifestations are visible in the `EXPLAIN PLAN` and are checked below.

DROP TABLE IF EXISTS t_loop_104863;

CREATE TABLE t_loop_104863 (c0 Int32) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_loop_104863 SELECT number FROM numbers(5);

-- With parallel replicas: the plan must use `Aggregating`, not `MergingAggregated`
-- (the latter is what triggered the exception, because `LoopSource` emits chunks
-- without `AggregatedChunkInfo`).
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

-- Without parallel replicas: the planner used to add no aggregation step at all
-- (silent wrong result). After the fix it adds a proper `Aggregating` step.
SELECT countIf(trimBoth(explain) = 'MergingAggregated') AS merging_aggregated_steps,
       countIf(trimBoth(explain) = 'Aggregating') AS aggregating_steps
FROM
(
    EXPLAIN PLAN
    SELECT c0
    FROM loop(remote('127.0.0.1', currentDatabase(), 't_loop_104863')) AS tx
    GROUP BY c0
    SETTINGS enable_parallel_replicas = 0
);

-- Sanity check: `loop(remote(...))` still produces rows and honours pushed-down
-- `LIMIT` (this exercises the `trivial_limit` path in `LoopSource`).
SELECT count() FROM
(
    SELECT c0
    FROM loop(remote('127.0.0.1', currentDatabase(), 't_loop_104863')) AS tx
    LIMIT 25
);

DROP TABLE t_loop_104863;
