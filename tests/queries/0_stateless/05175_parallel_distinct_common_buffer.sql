-- Scattering above a buffered common subplan requests data only when the consumer needs it, allowing
-- the decorrelation join to finish writing its shared buffer before the probe side reads it.

SELECT * % 1000 FROM numbers_mt(100000)
UNION DISTINCT
SELECT (SELECT number % 1000) FROM numbers_mt(100000)
SETTINGS max_threads = 4, max_block_size = 1000, limit = 1, allow_parallel_distinct = 1
FORMAT Null;

-- The result of unordered `LIMIT 1` is arbitrary. The pipeline still uses hash partitioning.
SELECT countIf(explain LIKE '%ScatterByPartition%') > 0
FROM (
    EXPLAIN PIPELINE
    SELECT * % 1000 FROM numbers_mt(100000)
    UNION DISTINCT
    SELECT (SELECT number % 1000) FROM numbers_mt(100000)
    SETTINGS max_threads = 4, max_block_size = 1000, limit = 1, allow_parallel_distinct = 1
);
