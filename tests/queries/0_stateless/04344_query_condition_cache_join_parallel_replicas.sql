-- Tags: no-parallel
-- no-parallel: asserts query condition cache hit counters, and the cache is instance-wide and bounded,
--              so a concurrent test can evict the warm-up entries between the two runs. The other
--              query condition cache tests that assert counters are serialized for the same reason.

-- Regression test for the parallel-replicas crash of the issue #104985 follow-up fix.
--
-- The first fix for #104985 discarded the cached index analysis in updatePrewhereInfo so it would be
-- recomputed with prewhere_info set. That forced a full primary-key/skip-index re-analysis at
-- pipeline-build time, which aborted on the parallel-replicas reading path (the index-analysis thread
-- pool ran a second time on a follower local plan). The fix re-runs only the cheap query condition
-- cache trimming instead, so the warm self-join must run under parallel replicas without crashing and
-- still return the correct result.

SET enable_analyzer = 1;
SET use_query_condition_cache = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

DROP TABLE IF EXISTS t_qcc_join_pr;

CREATE TABLE t_qcc_join_pr (x String, k UInt32)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0;

-- 1 mio rows: the query condition cache does not cache anything for small tables.
INSERT INTO t_qcc_join_pr SELECT concat('x-', toString(number % 1000)), number FROM numbers(1000000);

-- Warm-up run populates the cache (no parallel replicas, so the entries are written deterministically).
SELECT count() FROM (
    SELECT a.k FROM t_qcc_join_pr AS a
    INNER JOIN t_qcc_join_pr AS b ON a.k = b.k
    WHERE a.x = 'no_such_value_104985_pr' AND b.x = 'no_such_value_104985_pr'
) SETTINGS enable_parallel_replicas = 0;

-- Warm run under parallel replicas: before the fix this aborted the server in the index-analysis
-- re-run; after the fix it must complete and return 0 (the inner predicate matches no rows).
SELECT count() FROM (
    SELECT a.k FROM t_qcc_join_pr AS a
    INNER JOIN t_qcc_join_pr AS b ON a.k = b.k
    WHERE a.x = 'no_such_value_104985_pr' AND b.x = 'no_such_value_104985_pr'
)
SETTINGS
    -- Mode 2 throws instead of silently falling back to a local read, so the query failing to use
    -- parallel replicas is a test failure rather than a silent downgrade to the non-parallel path.
    allow_experimental_parallel_reading_from_replicas = 2,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_min_number_of_rows_per_replica = 10,
    parallel_replicas_local_plan = 1,
    log_comment = 'qcc_join_104985_pr';

SYSTEM FLUSH LOGS query_log;

-- Not crashing is not sufficient: the predicate matches no rows, so an ordinary full scan also
-- returns 0, and a cache hit with no selected rows is equally reachable from a plain local warm
-- query. Assert all three properties on the initiator row, so the run is pinned to the parallel
-- path and to being served from the query condition cache. Expected:
--   1  1  1   (parallel replicas used, hit, exactly 0 rows read)
SELECT
    ProfileEvents['ParallelReplicasQueryCount'] > 0,
    ProfileEvents['QueryConditionCacheHits'] > 0,
    ProfileEvents['SelectedRows'] = 0
FROM system.query_log
WHERE event_date >= yesterday()
    AND type = 'QueryFinish'
    AND is_initial_query
    AND current_database = currentDatabase()
    AND log_comment = 'qcc_join_104985_pr'
ORDER BY event_time_microseconds;

DROP TABLE t_qcc_join_pr;
