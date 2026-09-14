-- DISTINCT with a break limit returns a partial result from an unbounded input.
SELECT count() >= 1
FROM (SELECT DISTINCT number % 2 FROM system.numbers_mt)
SETTINGS max_threads = 4, allow_parallel_distinct = 1, max_rows_in_distinct = 1, distinct_overflow_mode = 'break', enable_analyzer = 1;
