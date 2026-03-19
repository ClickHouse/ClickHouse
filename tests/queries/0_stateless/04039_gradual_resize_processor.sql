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
