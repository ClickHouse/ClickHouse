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

-- The order-sensitive operation can also belong to the outer query. The final DISTINCT in
-- the derived table must not scatter its output before the outer OFFSET consumes it.
EXPLAIN PIPELINE
SELECT * FROM (SELECT DISTINCT number FROM numbers_mt(100000)) OFFSET 10
SETTINGS max_threads = 4, allow_parallel_distinct = 1;

-- The old interpreter must retain the same ordering guarantee.
EXPLAIN PIPELINE
SELECT * FROM (SELECT DISTINCT number FROM numbers_mt(100000)) OFFSET 10
SETTINGS max_threads = 4, allow_parallel_distinct = 1, enable_analyzer = 0;
