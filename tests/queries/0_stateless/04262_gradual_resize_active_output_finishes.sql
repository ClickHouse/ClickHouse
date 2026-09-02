-- Tags: no-random-settings
-- Regression for `GradualResizeProcessor` (`src/Processors/ResizeProcessor.cpp`,
-- `GradualResizeProcessor::prepare`, the branch that fires when an *active* output
-- finishes before the row/byte threshold is crossed): the processor must promote a
-- waiting inactive output so data keeps flowing and the query completes without
-- deadlock.
--
-- Force the scenario: pick a `min_rows_per_stream_for_gradual_resize` threshold large
-- enough that it never gets crossed by the input, so only the initial active output is
-- ever activated by the row-counter path. Then use `max_rows_to_group_by` with
-- `group_by_overflow_mode = 'break'` to make the active `AggregatingTransform` close its
-- input port (= the `GradualResize` output port) after a single chunk. At that moment
-- the threshold-driven promotion has not run, so the deadlock-avoidance branch in
-- `GradualResizeProcessor::prepare` must promote a waiting inactive output for the
-- pipeline to continue.
--
-- The pre-aggregation `resizeGradual` is only inserted when the source is not evenly
-- distributed (`AggregatingStep`: `!storage_has_evenly_distributed_read`). `numbers(...)`
-- reports `hasEvenlyDistributedRead = true` and bypasses `GradualResizeProcessor`
-- entirely, so this regression must read from a multi-stream `MergeTree` source.

DROP TABLE IF EXISTS test_gradual_resize_active;
CREATE TABLE test_gradual_resize_active (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 256;
INSERT INTO test_gradual_resize_active SELECT number % 1000, number FROM numbers(1000000);

SET enable_analyzer = 1;
SET min_rows_per_stream_for_gradual_resize = 100000000;
SET min_bytes_per_stream_for_gradual_resize = 0;
SET min_outstreams_per_resize_after_split = 0;
SET max_threads = 4;
-- `max_threads` is silently lowered to the number of threads that fit into the free memory
-- (`getMaxThreadsForAvailableMemory`), which on a loaded CI runner collapses the pipeline to a
-- single stream and removes every resize processor. Pin it off, the assertions below are about
-- the pipeline shape.
SET max_threads_min_free_memory_per_thread = 0;
-- The number of read streams is capped a second time by the minimum number of marks per
-- concurrent read, which is derived from `index_granularity_bytes` - a randomized `MergeTree`
-- setting. A small granularity in bytes makes that cap huge, collapses the read to a single stream
-- and removes every resize processor from the pipeline. Pin it off for the same reason.
SET merge_tree_min_rows_for_concurrent_read = 0;
SET merge_tree_min_bytes_for_concurrent_read = 0;
SET max_rows_to_group_by = 10;
SET group_by_overflow_mode = 'break';

-- Partial output under `break` mode is non-deterministic in row count, so we only check
-- that the query terminates and produces at least one aggregated row.
SELECT count() > 0 FROM
(
    SELECT k, count() AS c
    FROM test_gradual_resize_active
    GROUP BY k
);

-- Same scenario under split-resize: each split group has its own `GradualResizeProcessor`,
-- so the deadlock-avoidance branch must fire independently per group.
SET min_outstreams_per_resize_after_split = 4;
SET max_threads = 16;

SELECT count() > 0 FROM
(
    SELECT k, count() AS c
    FROM test_gradual_resize_active
    GROUP BY k
);

DROP TABLE test_gradual_resize_active;
