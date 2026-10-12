-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas; breaks GROUP BY correctness and cache ProfileEvents.

-- Partial aggregate cache: `partial_aggregate_cache_allow_parallel_aggregation_streams`; assert correct GROUP BY only (ProfileEvents vary by build).

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_partial_agg_cache_parallel;

CREATE TABLE test_partial_agg_cache_parallel (
    grp UInt8,
    value Int64
) ENGINE = MergeTree()
ORDER BY grp;

SYSTEM STOP MERGES test_partial_agg_cache_parallel;

SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

INSERT INTO test_partial_agg_cache_parallel SELECT 1, number FROM numbers(50000);
INSERT INTO test_partial_agg_cache_parallel SELECT 2, number FROM numbers(50000);

SELECT '--- Opt-in parallel aggregation streams: correct GROUP BY';

SELECT grp, sum(value), count()
FROM test_partial_agg_cache_parallel
GROUP BY grp
ORDER BY grp
SETTINGS
    use_partial_aggregate_cache = 1,
    partial_aggregate_cache_allow_parallel_aggregation_streams = 1,
    max_streams_for_merge_tree_reading = 16,
    max_threads = 16,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_cache_parallel_q1';

SELECT '--- Opt-in parallel streams: second run';

SELECT grp, sum(value), count()
FROM test_partial_agg_cache_parallel
GROUP BY grp
ORDER BY grp
SETTINGS
    use_partial_aggregate_cache = 1,
    partial_aggregate_cache_allow_parallel_aggregation_streams = 1,
    max_streams_for_merge_tree_reading = 16,
    max_threads = 16,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_cache_parallel_q2';

DROP TABLE test_partial_agg_cache_parallel;
