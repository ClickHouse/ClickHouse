-- Tags: no-parallel-replicas
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/110795
--
-- A bare non-UInt8 column in WHERE is an implicit-boolean filter (`a != 0`).
-- The projection read path (optimize_use_projections = 1) and the parallel-replicas
-- read path used to reject it with ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER (Code 59).

DROP TABLE IF EXISTS t_04371;

-- Table and projection match the issue: the projection groups by b, and the reproducer
-- filters on a, so the implicit-boolean filter column is not the projection key/output.
CREATE TABLE t_04371 (a UInt32, b UInt32, PROJECTION p (SELECT b, count() GROUP BY b))
ENGINE = MergeTree ORDER BY a;
INSERT INTO t_04371 SELECT number, number % 5 FROM numbers(100);

SELECT '-- normal path';
SELECT b, count() FROM t_04371 WHERE a GROUP BY b ORDER BY b
SETTINGS optimize_use_projections = 0;

-- Issue #110795 reproducer. WHERE a filters on a column not stored in the projection,
-- so the projection is not chosen (force_optimize_projection would throw PROJECTION_NOT_USED
-- here); enabling projections still triggered the Code 59 rejection during planning, so
-- result-parity with the normal path is the regression lock for this exact shape.
SELECT '-- issue #110795 reproducer';
SELECT b, count() FROM t_04371 WHERE a GROUP BY b ORDER BY b
SETTINGS optimize_use_projections = 1;

-- When the implicit-boolean filter is on the projection key the projection is chosen,
-- so force_optimize_projection = 1 deterministically asserts the projection path is taken.
SELECT '-- projection path (projection actually used)';
SELECT b, count() FROM t_04371 WHERE b GROUP BY b ORDER BY b
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    optimize_use_projections = 0;

-- The two result checks stay analyzer-agnostic so a randomized run verifies the same
-- rows with and without the analyzer.
SELECT '-- parallel replicas, local plan';
SELECT a, count() FROM t_04371 WHERE a GROUP BY a ORDER BY a LIMIT 5
SETTINGS parallel_replicas_local_plan = 1;

SELECT '-- parallel replicas, remote plan';
SELECT a, count() FROM t_04371 WHERE a GROUP BY a ORDER BY a LIMIT 5
SETTINGS parallel_replicas_local_plan = 0;

-- enable_analyzer = 1 is pinned only here, where the plan shape is asserted: the local
-- plan for parallel replicas engages only with the analyzer on
-- (canUseLocalPlanForParallelReplicas returns false otherwise), so without the pin a
-- randomized old-analyzer run would silently route the local-plan query through the
-- remote path and leave that half of the regression uncovered.
SELECT '-- local plan reads initiator locally';
SELECT countIf(explain ILIKE '%ReadFromMergeTree%') > 0 AND countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') > 0
FROM (EXPLAIN SELECT a, count() FROM t_04371 WHERE a GROUP BY a ORDER BY a LIMIT 5 SETTINGS parallel_replicas_local_plan = 1)
SETTINGS enable_analyzer = 1;

SELECT '-- remote plan reads all replicas remotely';
SELECT countIf(explain ILIKE '%ReadFromMergeTree%') = 0 AND countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') > 0
FROM (EXPLAIN SELECT a, count() FROM t_04371 WHERE a GROUP BY a ORDER BY a LIMIT 5 SETTINGS parallel_replicas_local_plan = 0)
SETTINGS enable_analyzer = 1;

DROP TABLE t_04371;
