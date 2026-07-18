-- Tags: no-parallel-replicas, no-darwin
-- no-darwin: STREAM reads are Linux-only (server raises SUPPORT_IS_DISABLED elsewhere).
-- no-parallel-replicas: STREAM reads do not support parallel replicas.

-- A pathological max_streams_for_merge_tree_reading must not throw std::length_error from
-- pipes.reserve in groupPartitionsByStreams (which aborts the server in debug/sanitizer builds).
-- EXPLAIN PIPELINE exercises the streaming read path without running the streaming query forever.

SET enable_streaming_queries = 1;

-- Keep effective `max_threads` as set below. Under memory pressure (e.g. per_test_coverage)
-- `getMaxThreadsForAvailableMemory` clamps `max_threads` down to 1, which stops the async branch
-- from amplifying `requested_num_streams`, so a buggy build would false-pass. See PR #100383.
SET max_threads_min_free_memory_per_thread = 0;

DROP TABLE IF EXISTS t_stream_max_streams_clamp;

CREATE TABLE t_stream_max_streams_clamp (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_stream_max_streams_clamp SELECT number FROM numbers(1000);

SELECT countIf(explain LIKE '%MergeTreeCommitOrderSequentialSource%') > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT count() FROM
    (
        (SELECT x FROM t_stream_max_streams_clamp GROUP BY ALL)
        EXCEPT DISTINCT
        (SELECT x FROM t_stream_max_streams_clamp STREAM GROUP BY ALL)
    )
    -- max_threads must be > 1: the async branch only amplifies requested_num_streams when
    -- it is not 1, so with max_threads = 1 the pathological setting never reaches reserve.
    SETTINGS max_threads = 4,
             max_streams_for_merge_tree_reading = 9223372036854775807,
             allow_asynchronous_read_from_io_pool_for_merge_tree = 1
);

-- requested_num_streams can also be amplified via max_streams * max_streams_to_max_threads_ratio
-- in the planner, independent of max_streams_for_merge_tree_reading (which defaults to 0). That
-- path must be bounded too, otherwise the same pipes.reserve throws the same std::length_error.
SELECT countIf(explain LIKE '%MergeTreeCommitOrderSequentialSource%') > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT count() FROM
    (
        (SELECT x FROM t_stream_max_streams_clamp GROUP BY ALL)
        EXCEPT DISTINCT
        (SELECT x FROM t_stream_max_streams_clamp STREAM GROUP BY ALL)
    )
    SETTINGS max_threads = 1025,
             max_streams_to_max_threads_ratio = 1e12,
             allow_asynchronous_read_from_io_pool_for_merge_tree = 1
);

DROP TABLE t_stream_max_streams_clamp;
