-- max_rows_to_read is the guard: a regressed short-circuit reads the unbounded side and trips it.
-- The DISTINCT modes are executed as joins by default, which build the right side before reading
-- the left one; this test covers the set-operation step.
SET optimize_rewrite_intersect_except_to_join = 0;
SELECT count()
FROM
(
    SELECT number FROM numbers(0)
    EXCEPT
    SELECT number FROM system.numbers
)
SETTINGS max_rows_to_read = 10000000, read_overflow_mode = 'throw', max_memory_usage = 50000000, max_untracked_memory = 1;

SELECT count()
FROM
(
    SELECT number FROM numbers(0)
    EXCEPT DISTINCT
    SELECT number FROM system.numbers
)
SETTINGS max_rows_to_read = 10000000, read_overflow_mode = 'throw', max_memory_usage = 50000000, max_untracked_memory = 1;
