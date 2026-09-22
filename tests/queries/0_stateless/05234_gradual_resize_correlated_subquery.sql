-- A `GROUP BY` inside a correlated subquery is rebuilt by the decorrelation with the correlated
-- columns appended to its keys (`PlannerCorrelatedSubqueries.cpp`). It is still the user's
-- aggregation, so `min_rows_per_stream_for_gradual_resize` / `min_bytes_per_stream_for_gradual_resize`
-- must keep applying to it: the rebuilt `AggregatingStep` has to carry the gradual-resize mark
-- over (`AggregatingStep::enableGradualResize`), otherwise the settings silently turn into a no-op
-- on this shape.
-- `numbers(...)` and `Memory` report `hasEvenlyDistributedRead = true` and bypass the pre-aggregation
-- resize entirely, so the aggregated source has to be a `MergeTree` table for the positive cases. The
-- settings are documented to ignore such sources, and the rebuilt step must honour that too: it is
-- built without the evenly-distributed-read property (its input is the decorrelating join), so it
-- would otherwise build a `GradualResize` that the same `GROUP BY` never builds on its own.

DROP TABLE IF EXISTS test_gradual_resize_outer;
DROP TABLE IF EXISTS test_gradual_resize_inner;
DROP TABLE IF EXISTS test_gradual_resize_inner_memory;

SET allow_experimental_correlated_subqueries = 1;
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
-- setting. Pin it off for the same reason.
SET merge_tree_min_rows_for_concurrent_read = 0;
SET merge_tree_min_bytes_for_concurrent_read = 0;
-- Aggregation in order takes a different pipeline branch that has no pre-aggregation resize.
SET optimize_aggregation_in_order = 0;

CREATE TABLE test_gradual_resize_outer (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE test_gradual_resize_inner (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 256;

CREATE TABLE test_gradual_resize_inner_memory (k UInt64, v UInt64) ENGINE = Memory;

INSERT INTO test_gradual_resize_outer SELECT number FROM numbers(10);
INSERT INTO test_gradual_resize_inner SELECT number % 10, number FROM numbers(200000);
INSERT INTO test_gradual_resize_inner_memory SELECT number % 10, number FROM numbers(200000);

-- Positive control: the subquery's `GROUP BY` takes the gradual path when it is not correlated.
SELECT count() > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT k, sum(v) FROM test_gradual_resize_inner GROUP BY k
)
WHERE explain LIKE '%GradualResize%';

-- The same `GROUP BY` inside a correlated subquery: the decorrelated plan must still build the
-- gradual resize for it.
SELECT count() > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT k
    FROM test_gradual_resize_outer
    WHERE EXISTS
    (
        SELECT v % 7 AS m, sum(v)
        FROM test_gradual_resize_inner
        WHERE test_gradual_resize_inner.k = test_gradual_resize_outer.k
        GROUP BY m
        HAVING sum(v) > 0
    )
)
WHERE explain LIKE '%GradualResize%';

-- And with the settings disabled the decorrelated aggregation keeps the strict resize.
SELECT countIf(explain LIKE '%GradualResize%'), countIf(explain LIKE '%Resize%') > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT k
    FROM test_gradual_resize_outer
    WHERE EXISTS
    (
        SELECT v % 7 AS m, sum(v)
        FROM test_gradual_resize_inner
        WHERE test_gradual_resize_inner.k = test_gradual_resize_outer.k
        GROUP BY m
        HAVING sum(v) > 0
    )
    SETTINGS min_rows_per_stream_for_gradual_resize = 0
);

-- An evenly distributed source (`Memory`) ignores the thresholds: neither the plain `GROUP BY` (which
-- plans no pre-aggregation resize at all) nor its decorrelated copy (which keeps the strict resize) may
-- build a `GradualResize`.
SELECT countIf(explain LIKE '%GradualResize%')
FROM
(
    EXPLAIN PIPELINE
    SELECT k, sum(v) FROM test_gradual_resize_inner_memory GROUP BY k
);

SELECT countIf(explain LIKE '%GradualResize%'), countIf(explain LIKE '%Resize%') > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT k
    FROM test_gradual_resize_outer
    WHERE EXISTS
    (
        SELECT v % 7 AS m, sum(v)
        FROM test_gradual_resize_inner_memory
        WHERE test_gradual_resize_inner_memory.k = test_gradual_resize_outer.k
        GROUP BY m
        HAVING sum(v) > 0
    )
);

-- The answer does not depend on the resize.
SELECT k, (SELECT count() FROM (SELECT v % 7 AS m, sum(v) FROM test_gradual_resize_inner WHERE test_gradual_resize_inner.k = test_gradual_resize_outer.k GROUP BY m HAVING sum(v) > 0)) AS groups
FROM test_gradual_resize_outer
ORDER BY k;

DROP TABLE test_gradual_resize_outer;
DROP TABLE test_gradual_resize_inner;
DROP TABLE test_gradual_resize_inner_memory;
