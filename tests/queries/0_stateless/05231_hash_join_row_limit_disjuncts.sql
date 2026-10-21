-- `max_rows_in_join` sums keys from every `OR` clause. With 100 rows per block and two clauses,
-- `break` must stop after the first block reaches 200 keys.
SELECT count()
FROM (SELECT number AS a, number * 3 + 1 AS b FROM numbers(300)) AS l
INNER JOIN (SELECT number AS a, number * 3 + 1 AS b FROM numbers(300)) AS r
ON l.a = r.a OR l.b = r.b
SETTINGS join_algorithm = 'hash', enable_analyzer = 1,
    query_plan_join_swap_table = 0, enable_join_runtime_filters = 0,
    max_threads = 1, max_block_size = 100,
    max_rows_in_join = 200, max_bytes_in_join = 0,
    join_overflow_mode = 'break',
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
