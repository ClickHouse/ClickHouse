-- Tags: no-random-settings

SET enable_analyzer = 1;

-- The assertions below are about the shape of, and the number of rows pushed through, the *local*
-- pre-aggregation pipeline. With parallel replicas the same rows are spread over several replicas,
-- so a single `GradualResizeProcessor` (or split group) sees only a fraction of the fixture and the
-- row thresholds picked below no longer describe what any one of them observes. The CI lane with
-- parallel replicas enabled therefore made the per-group threshold-scaling case below fail.
SET enable_parallel_replicas = 0;

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
CREATE TABLE test_gradual_resize (k UInt64, v UInt64) ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 256;
SYSTEM STOP MERGES test_gradual_resize;
INSERT INTO test_gradual_resize SELECT number % 10, number FROM numbers(0, 250000);
INSERT INTO test_gradual_resize SELECT number % 10, number FROM numbers(250000, 250000);
INSERT INTO test_gradual_resize SELECT number % 10, number FROM numbers(500000, 250000);
INSERT INTO test_gradual_resize SELECT number % 10, number FROM numbers(750000, 250000);

-- Keep this source non-partitioned: the recent partition-aggregation optimization can
-- skip the pre-aggregation resize completely for partition-local keys. With merges stopped,
-- the four inserts guarantee several source parts and streams, so this query reaches the
-- resize stage independently of single-part read heuristics.

SET min_rows_per_stream_for_gradual_resize = 1000;
SET min_bytes_per_stream_for_gradual_resize = 0;
SET max_threads = 4;
SET optimize_aggregation_in_order = 0;
-- `max_threads` is silently lowered to the number of threads that fit into the free memory
-- (`getMaxThreadsForAvailableMemory`), and the number of read streams is capped a second time by
-- the minimum number of marks per concurrent read. Either cap can collapse the pipeline to a
-- single stream, and `Pipe::resize` then early-returns for 1 -> 1, leaving no resize processor at
-- all. Pin all three off, the assertions below are about the pipeline shape.
SET max_threads_min_free_memory_per_thread = 0;
SET merge_tree_min_rows_for_concurrent_read = 0;
SET merge_tree_min_bytes_for_concurrent_read = 0;

SELECT count() > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT k, count()
    FROM test_gradual_resize
    GROUP BY k
)
WHERE explain LIKE '%GradualResize%';

-- Global aggregation has no grouping keys, so it must keep the strict resize even
-- when gradual-resize thresholds are enabled. Check both that `GradualResize` is
-- absent and that the ordinary `Resize` remains in the pipeline.
-- The aggregate has to read a column: `count()` alone is answered from the parts' metadata
-- (`optimize_trivial_count_query`) by a single source, and such a pipeline has no resize at all,
-- which would make the assertion below vacuously checkable only in its first half.
SELECT countIf(explain LIKE '%GradualResize%') = 0 AND countIf(explain LIKE '%Resize%') > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT sum(v)
    FROM test_gradual_resize
);

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

-- Execute the bytes-threshold path over the same MergeTree source. This covers
-- `Chunk::bytes` accounting and byte-threshold activation, rather than merely
-- checking that the planner inserted `GradualResize`.
SELECT k, count() AS c
FROM test_gradual_resize
GROUP BY k
ORDER BY k;

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
-- inactive waiting outputs while data is still flowing.
--
-- This case must read from the `MergeTree` source `test_gradual_resize`, NOT from `numbers(...)`:
-- `numbers` reports `hasEvenlyDistributedRead = true`, so `AggregatingStep` skips the
-- pre-aggregation `resizeGradual` entirely and the ramp-up path would never run (the case would
-- then pass even if the ramp-up / `inactive_waiting_outputs` promotion logic were broken or
-- removed). We assert the gradual path is present (`EXPLAIN PIPELINE`), so this proves the
-- ramp-up path is actually exercised, and we also verify the query completes and produces a
-- correct result.
SET min_rows_per_stream_for_gradual_resize = 100;
SET min_bytes_per_stream_for_gradual_resize = 0;
SET min_outstreams_per_resize_after_split = 0;
SET max_threads = 8;
SET optimize_aggregation_in_order = 0;

SELECT count() > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT k, sum(v) AS s
    FROM test_gradual_resize
    GROUP BY k
)
WHERE explain LIKE '%GradualResize%';

SELECT count() FROM
(
    SELECT k, sum(v) AS s
    FROM test_gradual_resize
    GROUP BY k
);

-- Per-group threshold scaling under split-resize: with split-resize active, `Pipe::resizeGradual`
-- builds one `GradualResizeProcessor` per split group and divides the global row/byte threshold
-- among the groups (`per_group_min_rows = 1 + (min_rows_per_output - 1) / groups`), so cumulative
-- activation across all groups still matches the documented global semantics.
--
-- This case must read from the `MergeTree` source `test_gradual_resize`, NOT from `numbers(...)`:
-- `numbers` reports `hasEvenlyDistributedRead = true`, so `AggregatingStep` skips the
-- pre-aggregation `resizeGradual` entirely and the split per-group path would never run.
--
-- The threshold is picked so that only the divided one can fire: the table has 1000000 rows, and
-- with `max_threads = 16` and `min_outstreams_per_resize_after_split = 4` there are 4 groups of 4
-- outputs, so each group's `GradualResizeProcessor` sees about 250000 rows. A global threshold of
-- 400000 is never reached by a single group, while the per-group threshold it is divided into
-- (100000) is crossed in every group. The pipeline shape alone cannot tell the two apart - a
-- `GradualResize × ` would be planned either way - so the observable is a runtime one: a group
-- that never activates feeds one of its outputs, and one more only if the deadlock-avoidance
-- branch promotes a waiting output, while a group that activates spreads the rest of its rows
-- over all of its outputs. Measured on the fixture below: 16 of 16 `AggregatingTransform` receive
-- rows with the division, 5 without it, so the check is "more than two per group".
SET min_rows_per_stream_for_gradual_resize = 400000;
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

SET log_processors_profiles = 1;

SELECT k, count() AS c
FROM test_gradual_resize
GROUP BY k
ORDER BY k
FORMAT Null SETTINGS log_comment = '04039_split_threshold_scaling';

SET log_processors_profiles = 0;

SYSTEM FLUSH LOGS processors_profile_log, query_log;

-- One `GradualResize` per split group, and more than two aggregation streams fed per group: without
-- the per-group division no group would ever activate its remaining outputs and the second value
-- would be `0`. The `event_time` bound keeps the log scans cheap: without it every flaky-check
-- rerun scans all the log rows accumulated by the earlier runs.
SELECT
    countIf(name = 'GradualResize') AS gradual_resizes,
    countIf(name = 'AggregatingTransform' AND input_rows > 0) > 2 * countIf(name = 'GradualResize') AS activated_the_remaining_outputs
FROM system.processors_profile_log AS p
INNER JOIN
(
    SELECT query_id
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
      AND current_database = currentDatabase() AND type = 'QueryFinish'
      AND log_comment = '04039_split_threshold_scaling'
) AS q ON p.query_id = q.query_id
WHERE p.event_date >= yesterday() AND p.event_time >= now() - INTERVAL 10 MINUTE;

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
