-- A correlated subquery shares its input through an in-memory `ChunkBuffer`: `SaveSubqueryResultToBuffer`
-- writes it, `ReadFromCommonBuffer` reads it, and the reader must run after every writer finished.
-- Scattering the final `DISTINCT` above such a reader used to make it run early, which threw
-- `Trying to extract chunk from ChunkBuffer before all inputs are finished`.
-- See https://github.com/ClickHouse/ClickHouse/issues/119375.

SELECT * % 1000 FROM numbers_mt(100000)
UNION DISTINCT
SELECT (SELECT number % 1000) FROM numbers_mt(100000)
SETTINGS max_threads = 4, max_block_size = 1000, limit = 1, enable_analyzer = 1, allow_parallel_distinct = 1
FORMAT Null;

-- Which row the `LIMIT 1` above returns is not deterministic, so it is not printed; the test is about the
-- query not throwing.

-- The `DISTINCT` over the buffered read is not scattered.
SELECT countIf(explain LIKE '%ScatterByPartition%')
FROM (
    EXPLAIN PIPELINE
    SELECT * % 1000 FROM numbers_mt(100000)
    UNION DISTINCT
    SELECT (SELECT number % 1000) FROM numbers_mt(100000)
    SETTINGS max_threads = 4, max_block_size = 1000, limit = 1, enable_analyzer = 1, allow_parallel_distinct = 1
);
