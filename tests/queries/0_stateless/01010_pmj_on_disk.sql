SET join_algorithm = 'hash';
SET query_plan_join_swap_table=false;

SELECT n, j FROM (SELECT number as n FROM numbers(4)) nums
ANY LEFT JOIN (
    SELECT number * 2 AS n, number + 10 AS j
    FROM numbers(4000)
) js2
USING n
ORDER BY n;

SET max_rows_in_join = 1000;

SELECT n, j FROM (SELECT number AS n FROM numbers(4)) nums
ANY LEFT JOIN (
    SELECT number * 2 AS n, number + 10 AS j
    FROM numbers(4000)
) js2
USING n
ORDER BY n; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SET join_algorithm = 'partial_merge';

SELECT n, j FROM (SELECT number as n FROM numbers(4)) nums
ANY LEFT JOIN (
    SELECT number * 2 AS n, number + 10 AS j
    FROM numbers(4000)
) js2
USING n
ORDER BY n;

SET partial_merge_join_optimizations = 1;

SELECT n, j FROM (SELECT number AS n FROM numbers(4)) nums
ANY LEFT JOIN (
    SELECT number * 2 AS n, number + 10 AS j
    FROM numbers(4000)
) js2
USING n
ORDER BY n;

SET join_algorithm = 'auto';

SELECT n, j FROM (SELECT number AS n FROM numbers(4)) nums
ANY LEFT JOIN (
    SELECT number * 2 AS n, number + 10 AS j
    FROM numbers(4000)
) js2
USING n
ORDER BY n;

SET max_rows_in_join = '10';

SELECT n, j FROM (SELECT number AS n FROM numbers(4)) nums
ANY LEFT JOIN (
    SELECT number * 2 AS n, number + 10 AS j
    FROM numbers(4000)
) js2
USING n
ORDER BY n;
