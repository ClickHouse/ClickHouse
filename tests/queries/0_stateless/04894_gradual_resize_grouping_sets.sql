-- Gradual resize of the `GROUP BY` pre-aggregation stage is documented as a no-op for
-- `GROUP BY ... GROUPING SETS ...`: that pipeline copies every stream into one branch per
-- grouping set and aggregates each branch separately, so it keeps the strict resize.
-- This test pins that contract, together with a positive control showing that the same
-- query shape does get `GradualResize` without `GROUPING SETS`.
-- `numbers(...)` reports `hasEvenlyDistributedRead = true` and bypasses the pre-aggregation
-- resize entirely, so the source has to be a `MergeTree` table.

DROP TABLE IF EXISTS test_gradual_resize_grouping_sets;
CREATE TABLE test_gradual_resize_grouping_sets (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 256;
-- 100000 rows are already several hundred granules, which is well past the point where the read
-- spreads over `max_threads` streams; a larger table only made the `GROUPING SETS` aggregation
-- below slow enough to exceed the per-test time limit of the debug build.
INSERT INTO test_gradual_resize_grouping_sets SELECT number % 10, number FROM numbers(100000);

SET min_rows_per_stream_for_gradual_resize = 1000;
SET min_bytes_per_stream_for_gradual_resize = 0;
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
-- Aggregation in order takes an entirely different pipeline branch that has no pre-aggregation
-- resize at all, so the positive control below would see no `GradualResize` with it enabled.
SET optimize_aggregation_in_order = 0;

-- Positive control: the ordinary keyed `GROUP BY` does use the gradual path.
SELECT count() > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT k, count()
    FROM test_gradual_resize_grouping_sets
    GROUP BY k
)
WHERE explain LIKE '%GradualResize%';

-- `GROUPING SETS` keeps the strict resize: no `GradualResize` in the pipeline.
SELECT count()
FROM
(
    EXPLAIN PIPELINE
    SELECT k, v % 3 AS m, count()
    FROM test_gradual_resize_grouping_sets
    GROUP BY GROUPING SETS ((k), (m))
)
WHERE explain LIKE '%GradualResize%';

-- The results of `GROUPING SETS` aggregation are unaffected by the settings:
-- 10 groups for `k` plus 3 groups for `m`, covering the 100000 rows twice.
SELECT count(), sum(c)
FROM
(
    SELECT k, v % 3 AS m, count() AS c
    FROM test_gradual_resize_grouping_sets
    GROUP BY GROUPING SETS ((k), (m))
);

DROP TABLE test_gradual_resize_grouping_sets;
