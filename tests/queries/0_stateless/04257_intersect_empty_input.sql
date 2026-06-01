-- Pin `max_threads = 1` so the server-level `additional_memory_tracking_per_thread`
-- speculative reservation (4 MiB by default) charges a single fixed offset on the
-- query memory tracker. With many pipeline workers the cumulative reservation can
-- reach the tight `max_memory_usage = 50000000` cap on its own (~12 threads × 4 MiB)
-- and raise `MEMORY_LIMIT_EXCEEDED` before the empty-input set operation completes,
-- even though the real memory footprint is negligible.

SELECT count()
FROM
(
    SELECT number FROM numbers(0)
    INTERSECT
    SELECT number FROM system.numbers
)
SETTINGS max_execution_time = 2, timeout_overflow_mode = 'throw', max_memory_usage = 50000000, max_untracked_memory = 1, max_threads = 1;

SELECT count()
FROM
(
    SELECT number FROM system.numbers
    INTERSECT
    SELECT number FROM numbers(0)
)
SETTINGS max_execution_time = 2, timeout_overflow_mode = 'throw', max_memory_usage = 50000000, max_untracked_memory = 1, max_threads = 1;

SELECT count()
FROM
(
    SELECT number FROM numbers(0)
    INTERSECT DISTINCT
    SELECT number FROM system.numbers
)
SETTINGS max_execution_time = 2, timeout_overflow_mode = 'throw', max_memory_usage = 50000000, max_untracked_memory = 1, max_threads = 1;

SELECT count()
FROM
(
    SELECT number FROM system.numbers
    INTERSECT DISTINCT
    SELECT number FROM numbers(0)
)
SETTINGS max_execution_time = 2, timeout_overflow_mode = 'throw', max_memory_usage = 50000000, max_untracked_memory = 1, max_threads = 1;
