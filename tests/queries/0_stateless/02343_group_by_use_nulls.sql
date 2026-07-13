set optimize_group_by_function_keys=0;
-- { echoOn }
SELECT number, number % 2, sum(number) AS val
FROM numbers(10)
GROUP BY ROLLUP(number, number % 2)
ORDER BY (number, number % 2, val)
SETTINGS group_by_use_nulls=1;

SELECT number, number % 2, sum(number) AS val
FROM numbers(10)
GROUP BY ROLLUP(number, number % 2)
ORDER BY (number, number % 2, val)
SETTINGS group_by_use_nulls=0;

SELECT number, number % 2, sum(number) AS val
FROM numbers(10)
GROUP BY CUBE(number, number % 2)
ORDER BY (number, number % 2, val)
SETTINGS group_by_use_nulls=1;

SELECT number, number % 2, sum(number) AS val
FROM numbers(10)
GROUP BY CUBE(number, number % 2)
ORDER BY (number, number % 2, val)
SETTINGS group_by_use_nulls=0;

SELECT
    number,
    number % 2,
    sum(number) AS val
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY (number, number % 2, val)
SETTINGS group_by_use_nulls = 1;

SELECT
    number,
    number % 2,
    sum(number) AS val
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY (number, number % 2, val)
SETTINGS group_by_use_nulls = 0;

SELECT number, number % 2, sum(number) AS val
FROM numbers(10)
GROUP BY ROLLUP(number, number % 2) WITH TOTALS
ORDER BY (number, number % 2, val)
SETTINGS group_by_use_nulls=1;

SELECT number, number % 2, sum(number) AS val
FROM numbers(10)
GROUP BY CUBE(number, number % 2) WITH TOTALS
ORDER BY (number, number % 2, val)
SETTINGS group_by_use_nulls=1;

SELECT
    number,
    number % 2,
    sum(number) AS val
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY 1, tuple(val)
SETTINGS group_by_use_nulls = 1, max_bytes_before_external_sort=10, max_bytes_ratio_before_external_sort=0;
