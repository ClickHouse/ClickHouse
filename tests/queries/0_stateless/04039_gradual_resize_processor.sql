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

DROP TABLE test_gradual_resize;

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

-- Edge case: an active downstream output finishes early before all outputs are activated.
-- LIMIT 1 closes the consumer almost immediately, so the active output transitions to
-- Finished while the gradual ramp-up is still in progress. The remaining outputs must
-- still receive data so the query terminates without deadlock.
SET min_rows_per_stream_for_gradual_resize = 100;
SET min_bytes_per_stream_for_gradual_resize = 0;
SET min_outstreams_per_resize_after_split = 0;
SET max_threads = 8;

SELECT count() FROM
(
    SELECT k, sum(number) AS s
    FROM (SELECT number % 1000 AS k, number FROM numbers(100000))
    GROUP BY k
    ORDER BY k
    LIMIT 1
);
