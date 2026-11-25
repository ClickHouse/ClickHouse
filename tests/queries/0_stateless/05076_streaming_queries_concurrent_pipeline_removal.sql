-- Tags: no-parallel-replicas
SET enable_analyzer = 1;
SET enable_streaming_queries = 1;

CREATE TABLE t_concurrent_pipeline_removal (n UInt64)
ENGINE = MergeTree PARTITION BY n % 32 ORDER BY n
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_concurrent_pipeline_removal SELECT number FROM numbers(1024);

-- Finished sources remove their pipelines while other threads still traverse graph edges.
-- Leave one row unread so the unbounded stream terminates without waiting for another insert.
SELECT count(), uniqExact(n) FROM
(
    SELECT n FROM t_concurrent_pipeline_removal STREAM LIMIT 1023
)
SETTINGS max_threads = 16, max_block_size = 1;

DROP TABLE t_concurrent_pipeline_removal;
