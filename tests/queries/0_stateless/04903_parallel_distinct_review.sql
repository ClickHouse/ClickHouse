-- `max_block_size` decides how many streams `numbers_mt` produces, and the CI randomizes it, so
-- every `EXPLAIN PIPELINE` below pins it to keep the number of streams deterministic.

-- A `break` limit reached by one partition must stop every partition. Otherwise a partition
-- that sees only duplicates keeps reading `system.numbers_mt` forever.
SELECT count() >= 1
FROM (SELECT DISTINCT number % 2 FROM system.numbers_mt)
SETTINGS max_threads = 4, allow_parallel_distinct = 1, max_rows_in_distinct = 1, distinct_overflow_mode = 'break', enable_analyzer = 1;

-- The `SETTINGS limit` is applied after the final UNION DISTINCT. It depends on the original
-- stream order, so the final DISTINCT must not repartition its input.
EXPLAIN PIPELINE
SELECT number % 1000 FROM numbers_mt(100000)
UNION DISTINCT
SELECT number % 1000 FROM numbers_mt(100000)
SETTINGS max_threads = 4, max_block_size = 1000, limit = 1, enable_analyzer = 1;

-- The order-sensitive operation can also belong to the outer query. The final DISTINCT in
-- the derived table must not scatter its output before the outer OFFSET consumes it.
EXPLAIN PIPELINE
SELECT * FROM (SELECT DISTINCT number FROM numbers_mt(100000)) OFFSET 10
SETTINGS max_threads = 4, max_block_size = 1000, allow_parallel_distinct = 1, enable_analyzer = 1;

-- The old interpreter must retain the same ordering guarantee.
EXPLAIN PIPELINE
SELECT * FROM (SELECT DISTINCT number FROM numbers_mt(100000)) OFFSET 10
SETTINGS max_threads = 4, max_block_size = 1000, allow_parallel_distinct = 1, enable_analyzer = 0;

-- The same holds for a set-operation DISTINCT inside a derived table: the outer OFFSET consumes
-- its stream order, so the final UNION DISTINCT must stay single-stream.
EXPLAIN PIPELINE
SELECT * FROM
(
    SELECT number % 1000 FROM numbers_mt(100000)
    UNION DISTINCT
    SELECT number % 1000 FROM numbers_mt(100000)
) OFFSET 10
SETTINGS max_threads = 4, max_block_size = 1000, allow_parallel_distinct = 1, enable_analyzer = 1;

EXPLAIN PIPELINE
SELECT * FROM
(
    SELECT number % 1000 FROM numbers_mt(100000)
    UNION DISTINCT
    SELECT number % 1000 FROM numbers_mt(100000)
) OFFSET 10
SETTINGS max_threads = 4, max_block_size = 1000, allow_parallel_distinct = 1, enable_analyzer = 0;

-- An outer LIMIT BY is order-sensitive in the same way.
EXPLAIN PIPELINE
SELECT * FROM
(
    SELECT number % 1000 AS k FROM numbers_mt(100000)
    UNION DISTINCT
    SELECT number % 1000 AS k FROM numbers_mt(100000)
) LIMIT 1 BY k
SETTINGS max_threads = 4, max_block_size = 1000, allow_parallel_distinct = 1, enable_analyzer = 1;

EXPLAIN PIPELINE
SELECT * FROM
(
    SELECT number % 1000 AS k FROM numbers_mt(100000)
    UNION DISTINCT
    SELECT number % 1000 AS k FROM numbers_mt(100000)
) LIMIT 1 BY k
SETTINGS max_threads = 4, max_block_size = 1000, allow_parallel_distinct = 1, enable_analyzer = 0;
