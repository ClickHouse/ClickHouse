-- https://github.com/ClickHouse/ClickHouse/issues/57728
-- Window functions require fully sorted data, so sort_overflow_mode = 'break' is overridden to 'throw'
-- for the sorting step used by window functions. This prevents the pipeline from getting stuck.

-- New analyzer path (Planner.cpp::addWindowSteps)
SELECT sum(a[length(a)]) FROM (SELECT groupArray(number) OVER (PARTITION BY number % 11 ORDER BY number) AS a FROM numbers_mt(10000)) FORMAT Null SETTINGS max_block_size = 8, max_rows_to_sort = 100, sort_overflow_mode = 'break', max_bytes_before_external_sort = 0, max_bytes_ratio_before_external_sort = 0; -- { serverError TOO_MANY_ROWS_OR_BYTES }

-- Old planner path (InterpreterSelectQuery::executeWindow), same fix must apply
SELECT sum(a[length(a)]) FROM (SELECT groupArray(number) OVER (PARTITION BY number % 11 ORDER BY number) AS a FROM numbers_mt(10000)) FORMAT Null SETTINGS max_block_size = 8, max_rows_to_sort = 100, sort_overflow_mode = 'break', max_bytes_before_external_sort = 0, max_bytes_ratio_before_external_sort = 0, enable_analyzer = 0; -- { serverError TOO_MANY_ROWS_OR_BYTES }
