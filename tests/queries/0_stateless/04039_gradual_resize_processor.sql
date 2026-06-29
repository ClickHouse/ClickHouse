-- Tags: no-random-settings

SET enable_analyzer = 1;

-- Verify correct GROUP BY results with GradualResize enabled (rows threshold)
SET min_rows_per_stream_for_gradual_resize = 1000;

SELECT number % 10 AS k, count() AS c
FROM numbers(10000)
GROUP BY k
ORDER BY k;

-- Verify correct GROUP BY results with GradualResize enabled (bytes threshold)
SET min_rows_per_stream_for_gradual_resize = 0;
SET min_bytes_per_stream_for_gradual_resize = 1000;

SELECT number % 5 AS k, sum(number) AS s
FROM numbers(1000)
GROUP BY k
ORDER BY k;

-- Verify EXPLAIN PIPELINE shows GradualResize processor
DROP TABLE IF EXISTS test_gradual_resize;
CREATE TABLE test_gradual_resize (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 256;
INSERT INTO test_gradual_resize SELECT number % 10, number FROM numbers(1000000);

SET min_rows_per_stream_for_gradual_resize = 1000;
SET min_bytes_per_stream_for_gradual_resize = 0;
SET max_threads = 4;

SELECT count() > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT k, count()
    FROM test_gradual_resize
    GROUP BY k
)
WHERE explain LIKE '%GradualResize%';

-- Verify the bytes-threshold path also inserts GradualResize (rows threshold disabled).
-- `numbers(...)` reports `hasEvenlyDistributedRead = true` and bypasses the pre-aggregation
-- resize entirely, so the bytes path must be exercised over a MergeTree source.
SET min_rows_per_stream_for_gradual_resize = 0;
SET min_bytes_per_stream_for_gradual_resize = 1000;

SELECT count() > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT k, count()
    FROM test_gradual_resize
    GROUP BY k
)
WHERE explain LIKE '%GradualResize%';

-- Verify split-resize is actually applied to the gradual path (regression guard for the interaction
-- between `min_rows_per_stream_for_gradual_resize` and `min_outstreams_per_resize_after_split`).
-- With split-resize active and enough upstream streams, `Pipe::resizeGradual` builds one
-- `GradualResizeProcessor` per split group. `EXPLAIN PIPELINE` collapses identical processors and
-- renders this as `GradualResize × G ...`; a single, non-split resize renders without the `× `
-- multiplier. `numbers(...)` reports `hasEvenlyDistributedRead = true` and bypasses the
-- pre-aggregation resize entirely, so a `MergeTree` source is required to exercise this path.
-- Matching `GradualResize × ` fails if split-resize is silently dropped from the gradual path
-- (it would degrade to a single `GradualResize`) or if the gradual path is dropped altogether.
SET min_rows_per_stream_for_gradual_resize = 1000;
SET min_bytes_per_stream_for_gradual_resize = 0;
SET min_outstreams_per_resize_after_split = 4;
SET max_threads = 16;
SET optimize_aggregation_in_order = 0;

SELECT count() > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT k, count()
    FROM test_gradual_resize
    GROUP BY k
)
WHERE explain LIKE '%GradualResize × %';

-- Verify GradualResize works correctly together with `min_outstreams_per_resize_after_split`.
-- The split-resize optimization reduces lock contention on `ExecutingGraph::Node::status_mutex`
-- at high parallelism; we must make sure the pipeline still produces correct results when both
-- knobs are enabled simultaneously.
SET min_rows_per_stream_for_gradual_resize = 1000;
SET min_bytes_per_stream_for_gradual_resize = 0;
SET min_outstreams_per_resize_after_split = 4;
SET max_threads = 16;

SELECT k, count() AS c
FROM (SELECT number % 10 AS k FROM numbers(100000))
GROUP BY k
ORDER BY k;

-- Stress the ramp-up phase: low threshold + high parallelism + many input chunks forces
-- `GradualResizeProcessor` to repeatedly cross the activation threshold and promote
-- inactive waiting outputs while data is still flowing. Verifies that the query completes
-- and produces a result.
SET min_rows_per_stream_for_gradual_resize = 100;
SET min_bytes_per_stream_for_gradual_resize = 0;
SET min_outstreams_per_resize_after_split = 0;
SET max_threads = 8;

SELECT count() FROM
(
    SELECT k, sum(number) AS s
    FROM (SELECT number % 1000 AS k, number FROM numbers(100000))
    GROUP BY k
);

-- Per-group threshold scaling under split-resize: with split-resize active, `Pipe::resizeGradual`
-- builds one `GradualResizeProcessor` per split group and divides the global row/byte threshold
-- among the groups (`per_group_min_rows = 1 + (min_rows_per_output - 1) / groups`), so cumulative
-- activation across all groups still matches the documented global semantics. The threshold here
-- (100000) is larger than the per-stream input, which exercises the per-group division branch and
-- the regime where, without split-resize, a single resize would never fully activate.
--
-- This case must read from the `MergeTree` source `test_gradual_resize`, NOT from `numbers(...)`:
-- `numbers` reports `hasEvenlyDistributedRead = true`, so `AggregatingStep` skips the
-- pre-aggregation `resizeGradual` entirely and the split per-group path would never run. We assert
-- that split-resize is still applied to the gradual path at this high threshold (`GradualResize × `,
-- which collapses the identical per-group processors), so this fails if split `resizeGradual` is
-- silently dropped from the gradual path, and we also check that results stay correct.
SET min_rows_per_stream_for_gradual_resize = 100000;
SET min_bytes_per_stream_for_gradual_resize = 0;
SET min_outstreams_per_resize_after_split = 4;
SET max_threads = 16;
SET optimize_aggregation_in_order = 0;

SELECT count() > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT k, count()
    FROM test_gradual_resize
    GROUP BY k
)
WHERE explain LIKE '%GradualResize × %';

SELECT k, count() AS c
FROM test_gradual_resize
GROUP BY k
ORDER BY k;

DROP TABLE test_gradual_resize;

-- Uneven split-resize groups: when `num_streams` is not divisible by the number of split
-- groups, `addSplitResizeTransform` pads the last group's outputs with `NullSink` and
-- inputs with `NullSource`. `GradualResizeProcessor` uses many-to-many routing and
-- activates all outputs once the threshold is crossed. The padded `NullSink` output is
-- finished immediately by `NullSink::prepare`, so it must never receive data after
-- activation; otherwise rows would be dropped. With `max_threads = 14` upstream streams
-- and `min_outstreams_per_resize_after_split = 4`, groups = 3 and each group has 5
-- ports — so the last group has one padded input wired to `NullSource` and one padded
-- output wired to `NullSink`. The query must still produce all input rows.
DROP TABLE IF EXISTS test_gradual_resize_split;
CREATE TABLE test_gradual_resize_split (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 256;
SYSTEM STOP MERGES test_gradual_resize_split;
INSERT INTO test_gradual_resize_split SELECT number,         number FROM numbers(10000);
INSERT INTO test_gradual_resize_split SELECT number+ 10000,  number FROM numbers(10000);
INSERT INTO test_gradual_resize_split SELECT number+ 20000,  number FROM numbers(10000);
INSERT INTO test_gradual_resize_split SELECT number+ 30000,  number FROM numbers(10000);
INSERT INTO test_gradual_resize_split SELECT number+ 40000,  number FROM numbers(10000);
INSERT INTO test_gradual_resize_split SELECT number+ 50000,  number FROM numbers(10000);
INSERT INTO test_gradual_resize_split SELECT number+ 60000,  number FROM numbers(10000);
INSERT INTO test_gradual_resize_split SELECT number+ 70000,  number FROM numbers(10000);
INSERT INTO test_gradual_resize_split SELECT number+ 80000,  number FROM numbers(10000);
INSERT INTO test_gradual_resize_split SELECT number+ 90000,  number FROM numbers(10000);
INSERT INTO test_gradual_resize_split SELECT number+100000,  number FROM numbers(10000);
INSERT INTO test_gradual_resize_split SELECT number+110000,  number FROM numbers(10000);
INSERT INTO test_gradual_resize_split SELECT number+120000,  number FROM numbers(10000);
INSERT INTO test_gradual_resize_split SELECT number+130000,  number FROM numbers(10000);

SET min_rows_per_stream_for_gradual_resize = 100;
SET min_bytes_per_stream_for_gradual_resize = 0;
SET min_outstreams_per_resize_after_split = 4;
SET max_threads = 14;
SET optimize_aggregation_in_order = 0;
SET max_block_size = 100;

SELECT sum(c) FROM
(
    SELECT k % 1000 AS k2, count() AS c
    FROM test_gradual_resize_split
    GROUP BY k2
);

DROP TABLE test_gradual_resize_split;
