SELECT count()
FROM
(
    SELECT number FROM numbers(0)
    EXCEPT
    SELECT number FROM system.numbers
)
SETTINGS max_execution_time = 2, timeout_overflow_mode = 'throw', max_memory_usage = 50000000, max_untracked_memory = 1;

SELECT count()
FROM
(
    SELECT number FROM numbers(0)
    EXCEPT DISTINCT
    SELECT number FROM system.numbers
)
SETTINGS max_execution_time = 2, timeout_overflow_mode = 'throw', max_memory_usage = 50000000, max_untracked_memory = 1;
