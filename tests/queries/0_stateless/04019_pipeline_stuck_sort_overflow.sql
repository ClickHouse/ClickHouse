-- https://github.com/ClickHouse/ClickHouse/issues/57728
-- The result is non-deterministic due to sort_overflow_mode = 'break', so we only verify the query completes.
SELECT sum(a[length(a)]) FROM (SELECT groupArray(number) OVER (PARTITION BY number % 11 ORDER BY number) AS a FROM numbers_mt(10000)) FORMAT Null SETTINGS max_block_size = 8, max_rows_to_sort = 100, sort_overflow_mode = 'break', max_bytes_before_external_sort = 0, max_bytes_ratio_before_external_sort = 0;
