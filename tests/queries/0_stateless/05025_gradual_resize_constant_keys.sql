-- Gradual resize of the `GROUP BY` pre-aggregation stage is skipped when every grouping key is a
-- constant: such an aggregation produces a single group, hence one partial state per stream
-- regardless of the data volume, exactly like a global aggregate. Throttling it would only
-- serialize the upstream scan without saving any merging work.
-- Constant keys are normally stripped by the analyzer, but not when the query has no aggregate
-- functions, and not on a remote shard that analyzes the query itself - both cases are covered
-- below.
-- `numbers(...)` reports `hasEvenlyDistributedRead = true` and bypasses the pre-aggregation resize
-- entirely, so the source has to be a `MergeTree` table.

DROP TABLE IF EXISTS test_gradual_resize_constant_keys;
CREATE TABLE test_gradual_resize_constant_keys (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 256;
INSERT INTO test_gradual_resize_constant_keys SELECT number % 10, number FROM numbers(1000000);

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
-- Aggregation in order takes a different pipeline branch that has no pre-aggregation resize.
SET optimize_aggregation_in_order = 0;

-- Positive control: an ordinary keyed `GROUP BY` does use the gradual path.
SELECT count() > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT k, count()
    FROM test_gradual_resize_constant_keys
    GROUP BY k
)
WHERE explain LIKE '%GradualResize%';

-- A constant grouping key is kept in the aggregation keys when the query has no aggregate
-- functions. It must keep the strict resize.
SELECT count()
FROM
(
    EXPLAIN PIPELINE
    SELECT 1 AS x
    FROM test_gradual_resize_constant_keys
    GROUP BY x
)
WHERE explain LIKE '%GradualResize%';

-- The strict resize is still there, only not the gradual one.
SELECT count() > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT 1 AS x
    FROM test_gradual_resize_constant_keys
    GROUP BY x
)
WHERE explain LIKE '%Resize%';

-- Results are unaffected: a single group covering all the rows.
SELECT x, c FROM (SELECT 1 AS x, count() AS c FROM test_gradual_resize_constant_keys GROUP BY x);

-- Distributed regression: a remote shard analyzes the query itself and keeps a constant grouping
-- key such as `hostName()` (`ExpressionAnalyzer` and `PlannerExpressionAnalysis` only strip it on
-- the initiator), so the shard-side pre-aggregation hits the same code path.
DROP TABLE IF EXISTS test_gradual_resize_constant_keys_dist;
CREATE TABLE test_gradual_resize_constant_keys_dist AS test_gradual_resize_constant_keys
ENGINE = Distributed(test_shard_localhost, currentDatabase(), test_gradual_resize_constant_keys);

SELECT count(), sum(c) FROM (SELECT hostName() AS h, count() AS c FROM test_gradual_resize_constant_keys_dist GROUP BY h);
SELECT count(), sum(c) FROM (SELECT 1 AS x, count() AS c FROM test_gradual_resize_constant_keys_dist GROUP BY x);

DROP TABLE test_gradual_resize_constant_keys_dist;
DROP TABLE test_gradual_resize_constant_keys;
