-- Gradual resize of the `GROUP BY` pre-aggregation stage is documented as a no-op for
-- `GROUP BY ... GROUPING SETS ...`: that pipeline copies every stream into one branch per
-- grouping set and aggregates each branch separately, so it keeps the strict resize.
-- This test pins that contract, together with a positive control showing that the same
-- query shape does get `GradualResize` without `GROUPING SETS`.
-- `numbers(...)` reports `hasEvenlyDistributedRead = true` and bypasses the pre-aggregation
-- resize entirely, so the source has to be a `MergeTree` table.

DROP TABLE IF EXISTS test_gradual_resize_grouping_sets;
CREATE TABLE test_gradual_resize_grouping_sets (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 256;
INSERT INTO test_gradual_resize_grouping_sets SELECT number % 10, number FROM numbers(1000000);

SET min_rows_per_stream_for_gradual_resize = 1000;
SET min_bytes_per_stream_for_gradual_resize = 0;
SET max_threads = 4;

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
-- 10 groups for `k` plus 3 groups for `m`, covering the 1000000 rows twice.
SELECT count(), sum(c)
FROM
(
    SELECT k, v % 3 AS m, count() AS c
    FROM test_gradual_resize_grouping_sets
    GROUP BY GROUPING SETS ((k), (m))
);

DROP TABLE test_gradual_resize_grouping_sets;
