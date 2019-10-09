SET max_memory_usage = 32000000;

SELECT number * 200000 as n, j FROM numbers(5)
ANY LEFT JOIN (
    SELECT number * 2 AS n, number AS j
    FROM numbers(1000000)
) js2
USING n; -- { serverError 241 }

SET partial_merge_join = 1;
SET default_max_bytes_in_join = 0;

SELECT number * 200000 as n, j FROM numbers(5)
ANY LEFT JOIN (
    SELECT number * 2 AS n, number AS j
    FROM numbers(1000000)
) js2
USING n; -- { serverError 12 }

SELECT number * 200000 as n, j FROM numbers(5)
ANY LEFT JOIN (
    SELECT number * 2 AS n, number AS j
    FROM numbers(1000000)
) js2
USING n
SETTINGS max_bytes_in_join = 30000000; -- { serverError 241 }

SELECT number * 200000 as n, j FROM numbers(5)
ANY LEFT JOIN (
    SELECT number * 2 AS n, number AS j
    FROM numbers(1000000)
) js2
USING n
ORDER BY n
SETTINGS max_bytes_in_join = 10000000;

SET partial_merge_join_optimizations = 1;
SET partial_merge_join_rows_in_left_blocks = 100000;

SELECT number * 200000 as n, j FROM numbers(5)
LEFT JOIN (
    SELECT number * 2 AS n, number AS j
    FROM numbers(1000000)
) js2
USING n
ORDER BY n
SETTINGS max_rows_in_join = 100000;

SET default_max_bytes_in_join = 10000000;

SELECT number * 200000 as n, j FROM numbers(5)
JOIN (
    SELECT number * 2 AS n, number AS j
    FROM numbers(1000000)
) js2
USING n
ORDER BY n;
