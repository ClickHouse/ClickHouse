-- max_rows_to_read is the guard: a regressed short-circuit reads the unbounded side and trips it.
-- max_threads bounds the read-ahead: a join reads a few probe blocks per stream before it sees an empty build side.
SELECT count()
FROM
(
    SELECT number FROM numbers(0)
    INTERSECT
    SELECT number FROM system.numbers
)
SETTINGS max_threads = 4, max_rows_to_read = 10000000, read_overflow_mode = 'throw', max_memory_usage = 50000000, max_untracked_memory = 1;

SELECT count()
FROM
(
    SELECT number FROM system.numbers
    INTERSECT
    SELECT number FROM numbers(0)
)
SETTINGS max_threads = 4, max_rows_to_read = 10000000, read_overflow_mode = 'throw', max_memory_usage = 50000000, max_untracked_memory = 1;

SELECT count()
FROM
(
    SELECT number FROM numbers(0)
    INTERSECT DISTINCT
    SELECT number FROM system.numbers
)
SETTINGS max_threads = 4, max_rows_to_read = 10000000, read_overflow_mode = 'throw', max_memory_usage = 50000000, max_untracked_memory = 1;

SELECT count()
FROM
(
    SELECT number FROM system.numbers
    INTERSECT DISTINCT
    SELECT number FROM numbers(0)
)
SETTINGS max_threads = 4, max_rows_to_read = 10000000, read_overflow_mode = 'throw', max_memory_usage = 50000000, max_untracked_memory = 1;
