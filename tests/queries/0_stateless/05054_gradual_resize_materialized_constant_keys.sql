-- Gradual resize of the `GROUP BY` pre-aggregation stage must be skipped when every grouping key is
-- semantically a constant, even when `materialize` strips the constness from the key column: such an
-- aggregation still produces a single group, hence one partial state per stream regardless of the
-- data volume, exactly like `GROUP BY 1` (see `05025_gradual_resize_constant_keys`).
-- `numbers(...)` reports `hasEvenlyDistributedRead = true` and bypasses the pre-aggregation resize
-- entirely, so the source has to be a `MergeTree` table.

DROP TABLE IF EXISTS test_gradual_resize_materialized_keys;
CREATE TABLE test_gradual_resize_materialized_keys (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 256;
INSERT INTO test_gradual_resize_materialized_keys SELECT number % 10, number FROM numbers(1000000);

SET min_rows_per_stream_for_gradual_resize = 1000;
SET min_bytes_per_stream_for_gradual_resize = 0;
SET max_threads = 4;
-- `max_threads` is silently lowered to the number of threads that fit into the free memory
-- (`getMaxThreadsForAvailableMemory`), which on a loaded CI runner collapses the pipeline to a
-- single stream and removes every resize processor. Pin it off, the assertions below are about
-- the pipeline shape.
SET max_threads_min_free_memory_per_thread = 0;
-- Aggregation in order takes a different pipeline branch that has no pre-aggregation resize.
SET optimize_aggregation_in_order = 0;

-- Positive control: an ordinary keyed `GROUP BY` does use the gradual path.
SELECT count() > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT k, sum(v)
    FROM test_gradual_resize_materialized_keys
    GROUP BY k
)
WHERE explain LIKE '%GradualResize%';

-- A materialized constant key is not a `ColumnConst` in the header, but the aggregation still
-- produces a single group. It must keep the strict resize.
SELECT count()
FROM
(
    EXPLAIN PIPELINE
    SELECT sum(v)
    FROM test_gradual_resize_materialized_keys
    GROUP BY materialize(1)
)
WHERE explain LIKE '%GradualResize%';

-- Same through an alias and with nesting.
SELECT count()
FROM
(
    EXPLAIN PIPELINE
    SELECT materialize(materialize(1)) AS x, sum(v)
    FROM test_gradual_resize_materialized_keys
    GROUP BY x
)
WHERE explain LIKE '%GradualResize%';

-- A mix of a materialized constant and a real key is not a single group - the gradual path applies.
SELECT count() > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT sum(v)
    FROM test_gradual_resize_materialized_keys
    GROUP BY materialize(1), k
)
WHERE explain LIKE '%GradualResize%';

-- The strict resize is still there for the materialized constant key, only not the gradual one.
SELECT count() > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT sum(v)
    FROM test_gradual_resize_materialized_keys
    GROUP BY materialize(1)
)
WHERE explain LIKE '%Resize%';

-- Results are unaffected: a single group covering all the rows.
SELECT count() FROM (SELECT sum(v) FROM test_gradual_resize_materialized_keys GROUP BY materialize(1));
SELECT sum(c) FROM (SELECT count() AS c FROM test_gradual_resize_materialized_keys GROUP BY materialize(1));

DROP TABLE test_gradual_resize_materialized_keys;
