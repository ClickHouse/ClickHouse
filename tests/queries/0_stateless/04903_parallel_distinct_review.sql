-- A `break` limit reached by one partition must stop every partition. Otherwise a partition
-- that sees only duplicates keeps reading `system.numbers_mt` forever.
SELECT count() >= 1
FROM (SELECT DISTINCT number % 2 FROM system.numbers_mt)
SETTINGS max_threads = 4, allow_parallel_distinct = 1, max_rows_in_distinct = 1, distinct_overflow_mode = 'break';

-- The `SETTINGS limit` is applied after the final UNION DISTINCT. It depends on the original
-- stream order, so the final DISTINCT must not repartition its input.
EXPLAIN PIPELINE
SELECT number % 1000 FROM numbers_mt(100000)
UNION DISTINCT
SELECT number % 1000 FROM numbers_mt(100000)
SETTINGS max_threads = 4, limit = 1;
