-- A test for #65647 that reduces the memory usage of some window functions.
-- without the optimization, about 174 MiB is used.
SELECT
    x,
    row_number() OVER (PARTITION BY x ORDER BY y ASC)
FROM
(
    SELECT
        number % 1 AS x,
        number AS y
    FROM numbers(10000000)
)
FORMAT `null`
SETTINGS max_threads = 1, max_memory_usage = 125829120;

SELECT
    x,
    rank() OVER (PARTITION BY x ORDER BY y ASC)
FROM
(
    SELECT
        number % 1 AS x,
        number AS y
    FROM numbers(10000000)
)
FORMAT `null`
SETTINGS max_threads = 1, max_memory_usage = 125829120;

SELECT
    x,
    dense_rank() OVER (PARTITION BY x ORDER BY y ASC)
FROM
(
    SELECT
        number % 1 AS x,
        number AS y
    FROM numbers(10000000)
)
FORMAT `null`
SETTINGS max_threads = 1, max_memory_usage = 125829120;
