-- Test for use_indexes_refiner_in_read_pools: mark ranges fully filtered out by a projection
-- index are dropped inside MergeTree read pools before read tasks are created for them.

SET optimize_use_projections = 1, optimize_use_projection_filtering = 1;
SET min_table_rows_to_use_projection_index = 0;
SET use_indexes_refiner_in_read_pools = 1;
-- Pin the plain pools by default (the prefetched and parallel-replicas pools are
-- enabled explicitly in dedicated queries below).
SET enable_parallel_replicas = 0;
SET allow_prefetched_read_pool_for_local_filesystem = 0, allow_prefetched_read_pool_for_remote_filesystem = 0;

DROP TABLE IF EXISTS t_proj_pools;

CREATE TABLE t_proj_pools
(
    id UInt64,
    region String,
    value UInt64,
    PROJECTION region_proj INDEX region TYPE basic
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 16, min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, enable_vertical_merge_algorithm = 0;

-- Two 'rare' clusters: rows 1600-1610 live in mark 100 and rows 1700-1710 in mark 106.
-- The 100-mark common prefix and the second cluster matter for the parallel replicas
-- in-order warmup check below.
INSERT INTO t_proj_pools
SELECT number, if(number BETWEEN 1600 AND 1610 OR number BETWEEN 1700 AND 1710, 'rare', 'common'), number * 10
FROM numbers(4096);

OPTIMIZE TABLE t_proj_pools FINAL;

-- Multi-threaded read: MergeTreeReadPool.
SELECT /* refiner_query_default_pool */ id, region, value FROM t_proj_pools WHERE region = 'rare' ORDER BY ALL
SETTINGS max_threads = 4, merge_tree_min_rows_for_concurrent_read = 256, merge_tree_min_read_task_size = 1, optimize_read_in_order = 0;

-- Single-threaded read in order of the primary key: MergeTreeReadPoolInOrder.
SELECT /* refiner_query_in_order_pool */ id, region, value FROM t_proj_pools WHERE region = 'rare' ORDER BY id LIMIT 5 SETTINGS max_threads = 1, optimize_read_in_order = 1;

-- Prefetched read pool: the refine-and-prefetch job never prefetches dropped ranges.
SELECT /* refiner_query_prefetched_pool */ id, region, value FROM t_proj_pools WHERE region = 'rare' ORDER BY ALL
SETTINGS max_threads = 4, merge_tree_min_rows_for_concurrent_read = 256, allow_prefetched_read_pool_for_local_filesystem = 1, local_filesystem_read_method = 'pread_threadpool', optimize_read_in_order = 0;

-- Parallel replicas over localhost: MergeTreeReadPoolParallelReplicas on every participant.
SELECT /* refiner_query_parallel_replicas_default */ id, region, value FROM t_proj_pools WHERE region = 'rare' ORDER BY ALL
SETTINGS max_threads = 4, merge_tree_min_rows_for_concurrent_read = 256, optimize_read_in_order = 0,
    enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1,
    -- projection support under parallel replicas requires a local plan and no aggregation-in-order
    parallel_replicas_local_plan = 1, optimize_aggregation_in_order = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- Parallel replicas reading in order: MergeTreeReadPoolParallelReplicasInOrder (WithOrder mode).
-- Single thread makes the sequence of tasks deterministic (no reading ahead of the LIMIT).
SELECT /* refiner_query_parallel_replicas_in_order */ id, region, value FROM t_proj_pools WHERE region = 'rare' ORDER BY id LIMIT 5
SETTINGS max_threads = 1, optimize_read_in_order = 1,
    enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1,
    -- projection support under parallel replicas requires a local plan and no aggregation-in-order
    parallel_replicas_local_plan = 1, optimize_aggregation_in_order = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- The same pool in ReverseOrder mode.
SELECT /* refiner_query_parallel_replicas_reverse */ id, region, value FROM t_proj_pools WHERE region = 'rare' ORDER BY id DESC LIMIT 5
SETTINGS max_threads = 4, optimize_read_in_order = 1,
    enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1,
    -- projection support under parallel replicas requires a local plan and no aggregation-in-order
    parallel_replicas_local_plan = 1, optimize_aggregation_in_order = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- Correctness on a value that does not exist at all.
SELECT count() FROM t_proj_pools WHERE region = 'nonexistent';

SYSTEM FLUSH LOGS query_log;

-- The part has 256 marks and all matching rows live in two of them,
-- so the full scan must drop almost all marks at task-cut time.
-- Do not assert on ReadPoolRangeRefinerDroppedCuts: whether a cut is dropped as a whole
-- depends on the task sizing regime (storage type, stream count, read method), which is
-- environment-dependent and randomized in CI.
SELECT
    ProfileEvents['ReadPoolRangeRefinerDroppedMarks'] > 200 AS dropped_marks
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%refiner_query_default_pool%'
    AND query NOT LIKE '%query_log%';

-- Read-in-order now runs before projection selection, and an in-order read declines
-- projections entirely (`canUseProjectionForReadingStep`), so the projection-index
-- refiner does not attach to in-order read pools and nothing is dropped.
SELECT
    ProfileEvents['ReadPoolRangeRefinerDroppedMarks'] > 0 AS dropped_marks
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%refiner_query_in_order_pool%'
    AND query NOT LIKE '%query_log%';

-- The prefetched pool must drop the same marks as the default pool (task boundaries are
-- pre-split there, so do not assert on the number of fully dropped tasks).
SELECT
    ProfileEvents['ReadPoolRangeRefinerDroppedMarks'] > 200 AS dropped_marks
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%refiner_query_prefetched_pool%'
    AND query NOT LIKE '%query_log%';

-- Under parallel replicas the marks are spread over the participants, so sum the events
-- over all queries initiated by the marked one. ParallelReplicasReadMarks counts marks
-- after refinement (only the ones which reach a reader): 2 surviving marks, not 256.
SELECT
    sum(ProfileEvents['ReadPoolRangeRefinerDroppedMarks']) > 200 AS dropped_marks,
    sum(ProfileEvents['ParallelReplicasReadMarks']) < 10 AS read_marks_counted_after_refinement
FROM system.query_log
WHERE type = 'QueryFinish'
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND is_initial_query
            AND query LIKE '%refiner_query_parallel_replicas_default%'
            AND query NOT LIKE '%query_log%');

-- An in-order read declines projections (see above), so the refiner does not attach and
-- no marks are dropped. The former warmup-task-size check is gone with the refiner: without
-- refinement there are no dropped cuts to inflate the warmup task, and the marks read vary
-- with whether parallel replicas engage at all, so nothing stable is left to assert on.
SELECT
    sum(ProfileEvents['ReadPoolRangeRefinerDroppedMarks']) > 0 AS dropped_marks
FROM system.query_log
WHERE type = 'QueryFinish'
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND is_initial_query
            AND query LIKE '%refiner_query_parallel_replicas_in_order%'
            AND query NOT LIKE '%query_log%');

-- The reverse in-order read also declines projections, so nothing is dropped here either.
SELECT sum(ProfileEvents['ReadPoolRangeRefinerDroppedMarks']) > 100 AS dropped_marks
FROM system.query_log
WHERE type = 'QueryFinish'
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND is_initial_query
            AND query LIKE '%refiner_query_parallel_replicas_reverse%'
            AND query NOT LIKE '%query_log%');

DROP TABLE t_proj_pools;
