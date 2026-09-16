-- Tags: no-fasttest, no-parallel-replicas
-- no-fasttest: the vector similarity index is not compiled into the Fast test build.
-- no-parallel-replicas: with parallel replicas the vector search optimization is disabled and the
-- read layer uses a different pool, so the concurrency this test pins would not exist.

-- Several threads search a vector similarity index over ONE part, so that the per-part vector
-- search read hints are written and consumed while more than one reader is alive for that part. The
-- assertions pin the split across readers, the index search, and equality with a single threaded read.

-- The test runner can enable the query result cache. A cache hit returns an earlier attempt's answer
-- without building a plan or reading the part, which would make every assertion below replay a stale
-- result instead of measuring this attempt. Session wide, so a future assertion cannot forget it.
SET use_query_cache = 0;

-- The test runner can inject a `compatibility` value below 25.1, which reverts
-- `query_plan_try_use_vector_search` to false and turns the vector search optimization off. Every
-- query below forces `idx`, so the whole test would fail with INDEX_NOT_USED on a healthy build.
-- Session wide, so it also covers the statements that only read system tables.
SET query_plan_try_use_vector_search = 1;

-- ignore_drop_queries_probability = 0: the stress runner sets it to 0.2, which makes a DROP a no-op.
DROP TABLE IF EXISTS vs_concurrent SETTINGS ignore_drop_queries_probability = 0;

CREATE TABLE vs_concurrent
(
    id UInt32,
    grp UInt8,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 8) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
-- Both are explicit, so that setting randomization cannot change the mark count: many marks in one
-- part, so that the read pool can split the part across threads. index_granularity alone does not
-- pin it, because the adaptive granule size is min(index_granularity, index_granularity_bytes /
-- bytes_per_row) and randomization lowers the byte divisor. The default byte value is kept, so the
-- part stays in the ordinary adaptive regime rather than switching to non-adaptive marks.
SETTINGS index_granularity = 128, index_granularity_bytes = 10485760;

-- Distance from the origin grows strictly with id, so the top-k is unique and no tie-break sort key
-- is needed. A second ORDER BY key would disable the vector search optimization.
-- 2560 rows is 21 marks at the granularity pinned above, which is enough for the read pool to
-- hand tasks to several threads: peak_threads_usage reaches 5 here, against the > 2 the assertions
-- below require. It is also the smallest round size that keeps that margin -- 1024 rows is 9 marks
-- and peaks at exactly 2 threads, which fails those assertions outright.
INSERT INTO vs_concurrent
SELECT number, number % 4,
       arrayMap(j -> if(j = 0, toFloat32(number) / 1000.0, toFloat32(0)), range(8))
FROM numbers(2560);

OPTIMIZE TABLE vs_concurrent FINAL;

-- Several readers must share ONE part, otherwise the concurrency below could come from several parts.
SELECT 'one_part', count() FROM system.parts
WHERE database = currentDatabase() AND table = 'vs_concurrent' AND active;

-- Settings used by every measured query below, and why each is needed:
--   merge_tree_min_rows_for_concurrent_read = 1  one part must be split across read tasks, so that
--   merge_tree_min_bytes_for_concurrent_read = 1  several readers are created for it. The minimum
--                                                task size is the larger of the two thresholds
--                                                divided by the respective granularity, so BOTH must
--                                                be pinned: against this 21 mark part the rows arm
--                                                asks for 1280 marks at its default and the bytes
--                                                arm for 24, so either one left at its default
--                                                still reduces the part to a single stream.
--                                                Measured: pinning both yields 3 readers, pinning
--                                                only the rows arm yields 0
--   max_threads = 4                              the number of concurrent readers
--   use_concurrency_control = 0                  the executor downscales worker threads when CPU
--                                                slots are scarce, which would leave the read
--                                                single-threaded while the plan still looks wide
--   max_threads_min_free_memory_per_thread = 0   max_threads is lowered under memory pressure
--   enable_parallel_replicas = 0                 the test runner can inject parallel replicas, which
--                                                disables the vector search optimization
--   allow_prefetched_read_pool_for_*_filesystem = 0
--                                                the assertions below match the default read pool by
--                                                name; the prefetched pool has a different name
--   force_data_skipping_indices = 'idx'          the query must fail rather than silently degrade to
--                                                a brute force scan
--   use_query_cache = 0                          also pinned per statement, on top of the session wide
--                                                SET above, because these three are the statements
--                                                read back from system.query_log
-- The runner injects query_cache_system_table_handling = 'ignore' in the same block that enables the
-- cache, which is why the two statements below that read system tables never hit the
-- QUERY_CACHE_USED_WITH_SYSTEM_TABLE throw that setting raises at its default.

-- vector_search_with_rescoring = 0: the vector column is replaced by _distance, which is the first
-- clause of the gate that stores per-part read hints.

SELECT 'concurrent_readers', sumIf(
    toUInt64OrDefault(extract(explain, '× (\d+)'), toUInt64(1)),
    explain LIKE '%MergeTreeSelect(pool: ReadPool,%') >= 2
FROM (
    EXPLAIN PIPELINE
    SELECT id FROM vs_concurrent ORDER BY L2Distance(vec, [0., 0., 0., 0., 0., 0., 0., 0.]) LIMIT 5
    SETTINGS merge_tree_min_rows_for_concurrent_read = 1,
             merge_tree_min_bytes_for_concurrent_read = 1, max_threads = 4,
             use_concurrency_control = 0, max_threads_min_free_memory_per_thread = 0,
             enable_parallel_replicas = 0,
             allow_prefetched_read_pool_for_remote_filesystem = 0,
             allow_prefetched_read_pool_for_local_filesystem = 0,
             force_data_skipping_indices = 'idx', vector_search_with_rescoring = 0
);

SELECT 'index_used_distance', countIf(explain LIKE '%_distance%') > 0
FROM (
    EXPLAIN actions = 1
    SELECT id FROM vs_concurrent ORDER BY L2Distance(vec, [0., 0., 0., 0., 0., 0., 0., 0.]) LIMIT 5
    SETTINGS merge_tree_min_rows_for_concurrent_read = 1,
             merge_tree_min_bytes_for_concurrent_read = 1, max_threads = 4,
             use_concurrency_control = 0, max_threads_min_free_memory_per_thread = 0,
             enable_parallel_replicas = 0,
             allow_prefetched_read_pool_for_remote_filesystem = 0,
             allow_prefetched_read_pool_for_local_filesystem = 0,
             force_data_skipping_indices = 'idx', vector_search_with_rescoring = 0
);

-- The three queries below are the ones that actually execute, so their log_comment is read back from
-- system.query_log further down. The settings must sit on the top level statement: a log_comment
-- inside a subquery's SETTINGS does not reach system.query_log.
-- The ids are compared as a sorted set: groupArray over a subquery does not preserve the subquery's
-- ORDER BY, so asserting the order would depend on unrelated settings.
-- The id set is also what makes the first query below an oracle for the read hints being consumed:
-- with vector_search_with_rescoring = 0 the sort key is the _distance virtual column, filled only
-- from the per-part hints, so a reader that does not consume them sorts by a constant and returns
-- other ids. _distance itself cannot be selected to assert directly, it is rejected with
-- ILLEGAL_COLUMN (see 02354_vector_search_incident1654.sql).

SELECT 'exact_topk', arraySort(groupArray(id)) = [0, 1, 2, 3, 4]
FROM (SELECT id FROM vs_concurrent ORDER BY L2Distance(vec, [0., 0., 0., 0., 0., 0., 0., 0.]) LIMIT 5)
SETTINGS merge_tree_min_rows_for_concurrent_read = 1,
         merge_tree_min_bytes_for_concurrent_read = 1, max_threads = 4,
         use_concurrency_control = 0, max_threads_min_free_memory_per_thread = 0,
         enable_parallel_replicas = 0,
         allow_prefetched_read_pool_for_remote_filesystem = 0,
         allow_prefetched_read_pool_for_local_filesystem = 0,
         force_data_skipping_indices = 'idx', vector_search_with_rescoring = 0,
         use_query_cache = 0,
         log_comment = '02354_vector_search_concurrent_readers_r0';

-- vector_search_with_rescoring = 1 (not the default): the vector column is kept and rows are filtered
-- instead, which reaches the gate's second clause, use_vector_search_result_filter. So this is a
-- second carrier of the same state, not a variant of the first.
SELECT 'topk_rescoring', arraySort(groupArray(id)) = [0, 1, 2, 3, 4]
FROM (SELECT id FROM vs_concurrent ORDER BY L2Distance(vec, [0., 0., 0., 0., 0., 0., 0., 0.]) LIMIT 5)
SETTINGS merge_tree_min_rows_for_concurrent_read = 1,
         merge_tree_min_bytes_for_concurrent_read = 1, max_threads = 4,
         use_concurrency_control = 0, max_threads_min_free_memory_per_thread = 0,
         enable_parallel_replicas = 0,
         allow_prefetched_read_pool_for_remote_filesystem = 0,
         allow_prefetched_read_pool_for_local_filesystem = 0,
         force_data_skipping_indices = 'idx', vector_search_with_rescoring = 1,
         use_query_cache = 0,
         log_comment = '02354_vector_search_concurrent_readers_r1';

-- Row filtering, which drives the rescoring row filter consumers under concurrent readers. Only the
-- count is asserted: with a row filter the approximate search picks candidates per granule, and which
-- ones it picks is not stable across index builds, so the exact id set cannot be pinned here.
SELECT 'topk_filtered', length(groupArray(id)) = 5
FROM (SELECT id FROM vs_concurrent WHERE grp = 1
      ORDER BY L2Distance(vec, [0., 0., 0., 0., 0., 0., 0., 0.]) LIMIT 5)
SETTINGS merge_tree_min_rows_for_concurrent_read = 1,
         merge_tree_min_bytes_for_concurrent_read = 1, max_threads = 4,
         use_concurrency_control = 0, max_threads_min_free_memory_per_thread = 0,
         enable_parallel_replicas = 0,
         allow_prefetched_read_pool_for_remote_filesystem = 0,
         allow_prefetched_read_pool_for_local_filesystem = 0,
         force_data_skipping_indices = 'idx', vector_search_with_rescoring = 1,
         use_query_cache = 0,
         log_comment = '02354_vector_search_concurrent_readers_filtered';

SYSTEM FLUSH LOGS query_log;

-- EXPLAIN PIPELINE above shows how wide the plan is, not that anything ran on more than one thread.
-- This reads the three executed queries instead, asserting three things about each:
--   length(thread_ids) > 1        more than one thread was ever attached. thread_ids is cumulative,
--                                "threads which has been attached to the group", so it does not say
--                                they overlapped in time, and a single stream read already reads 2
--                                because PullingAsyncPipelineExecutor::pull always spawns one thread
--                                outside the max_threads limit (01283_max_threads_simple_query_
--                                optimization.sql, 02871_peak_threads_usage.sh). Kept as that weaker
--                                statement only
--   USearchSearchCount > 0       a real index search happened rather than a brute force scan
--   peak_threads_usage > 2       readers ran CONCURRENTLY: this is the maximum number of threads
--                                simultaneously attached, which a single stream execution cannot
--                                reach. This is the column that carries the concurrency claim
-- Inequalities, not exact counts, because the thread count is host dependent. Counting to 3 also
-- catches a missing row.
-- The window has to hold exactly this attempt's rows: the test runner re-runs a whole test on retry
-- and does not recreate a fixed --database, so an earlier attempt's rows are still visible. Counting
-- them too would over-count, and taking only the newest three would under-count just as badly, by
-- padding a missing row with a healthy one from the earlier attempt. So the window starts at this
-- attempt's own CREATE TABLE, of which there is exactly one and it precedes every measured query.
-- The log_comment values are listed exactly rather than matched by prefix, so that a future test
-- whose file name starts with this one's cannot enter the window either. is_internal = 0 is the
-- established idiom for excluding server-issued queries (01702_system_query_log.sql,
-- 03148_query_log_used_dictionaries.sql).
SELECT 'threads_and_index',
       countIf(length(thread_ids) > 1) = 3,
       countIf(ProfileEvents['USearchSearchCount'] > 0) = 3,
       countIf(peak_threads_usage > 2) = 3
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND is_internal = 0
  AND log_comment IN ('02354_vector_search_concurrent_readers_r0',
                      '02354_vector_search_concurrent_readers_r1',
                      '02354_vector_search_concurrent_readers_filtered')
  AND event_time_microseconds > (
      SELECT max(event_time_microseconds)
      FROM system.query_log
      WHERE event_date >= yesterday() AND current_database = currentDatabase()
        AND type = 'QueryFinish' AND query_kind = 'Create'
        AND has(tables, currentDatabase() || '.vs_concurrent')
  );

SELECT 'concurrent_readers_rescoring', sumIf(
    toUInt64OrDefault(extract(explain, '× (\d+)'), toUInt64(1)),
    explain LIKE '%MergeTreeSelect(pool: ReadPool,%') >= 2
FROM (
    EXPLAIN PIPELINE
    SELECT id FROM vs_concurrent ORDER BY L2Distance(vec, [0., 0., 0., 0., 0., 0., 0., 0.]) LIMIT 5
    SETTINGS merge_tree_min_rows_for_concurrent_read = 1,
             merge_tree_min_bytes_for_concurrent_read = 1, max_threads = 4,
             use_concurrency_control = 0, max_threads_min_free_memory_per_thread = 0,
             enable_parallel_replicas = 0,
             allow_prefetched_read_pool_for_remote_filesystem = 0,
             allow_prefetched_read_pool_for_local_filesystem = 0,
             force_data_skipping_indices = 'idx', vector_search_with_rescoring = 1
);

SELECT 'concurrent_readers_filtered', sumIf(
    toUInt64OrDefault(extract(explain, '× (\d+)'), toUInt64(1)),
    explain LIKE '%MergeTreeSelect(pool: ReadPool,%') >= 2
FROM (
    EXPLAIN PIPELINE
    SELECT id FROM vs_concurrent WHERE grp = 1
    ORDER BY L2Distance(vec, [0., 0., 0., 0., 0., 0., 0., 0.]) LIMIT 5
    SETTINGS merge_tree_min_rows_for_concurrent_read = 1,
             merge_tree_min_bytes_for_concurrent_read = 1, max_threads = 4,
             use_concurrency_control = 0, max_threads_min_free_memory_per_thread = 0,
             enable_parallel_replicas = 0,
             allow_prefetched_read_pool_for_remote_filesystem = 0,
             allow_prefetched_read_pool_for_local_filesystem = 0,
             force_data_skipping_indices = 'idx', vector_search_with_rescoring = 1
);

-- Concurrency must not change the answer. Not compared against a brute force scan: approximate
-- nearest neighbour search may legitimately differ from it, especially with a row filter.

SELECT 'diff_rescoring', (
    SELECT arraySort(groupArray(id)) FROM (
        SELECT id FROM vs_concurrent ORDER BY L2Distance(vec, [0., 0., 0., 0., 0., 0., 0., 0.]) LIMIT 5
        SETTINGS merge_tree_min_rows_for_concurrent_read = 1,
                 merge_tree_min_bytes_for_concurrent_read = 1, max_threads = 4,
                 use_concurrency_control = 0, max_threads_min_free_memory_per_thread = 0,
                 enable_parallel_replicas = 0,
                 allow_prefetched_read_pool_for_remote_filesystem = 0,
                 allow_prefetched_read_pool_for_local_filesystem = 0,
                 force_data_skipping_indices = 'idx', vector_search_with_rescoring = 1
    )
) = (
    SELECT arraySort(groupArray(id)) FROM (
        SELECT id FROM vs_concurrent ORDER BY L2Distance(vec, [0., 0., 0., 0., 0., 0., 0., 0.]) LIMIT 5
        SETTINGS max_threads = 1, enable_parallel_replicas = 0,
                 force_data_skipping_indices = 'idx', vector_search_with_rescoring = 1
    )
);

SELECT 'diff_filtered', (
    SELECT arraySort(groupArray(id)) FROM (
        SELECT id FROM vs_concurrent WHERE grp = 1
        ORDER BY L2Distance(vec, [0., 0., 0., 0., 0., 0., 0., 0.]) LIMIT 5
        SETTINGS merge_tree_min_rows_for_concurrent_read = 1,
                 merge_tree_min_bytes_for_concurrent_read = 1, max_threads = 4,
                 use_concurrency_control = 0, max_threads_min_free_memory_per_thread = 0,
                 enable_parallel_replicas = 0,
                 allow_prefetched_read_pool_for_remote_filesystem = 0,
                 allow_prefetched_read_pool_for_local_filesystem = 0,
                 force_data_skipping_indices = 'idx', vector_search_with_rescoring = 1
    )
) = (
    SELECT arraySort(groupArray(id)) FROM (
        SELECT id FROM vs_concurrent WHERE grp = 1
        ORDER BY L2Distance(vec, [0., 0., 0., 0., 0., 0., 0., 0.]) LIMIT 5
        SETTINGS max_threads = 1, enable_parallel_replicas = 0,
                 force_data_skipping_indices = 'idx', vector_search_with_rescoring = 1
    )
);

DROP TABLE vs_concurrent SETTINGS ignore_drop_queries_probability = 0;
