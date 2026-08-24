SET enable_analyzer=1;

-- { echoOn }
SELECT number, number % 2, sum(number) AS val
FROM numbers(10)
GROUP BY ROLLUP(number, number % 2)
ORDER BY (number, number % 2, val)
SETTINGS group_by_use_nulls=1;

set optimize_group_by_function_keys = 0;

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

CREATE TABLE test
ENGINE = ReplacingMergeTree
PRIMARY KEY id
AS SELECT number AS id FROM numbers(100);

SELECT id
FROM test
GROUP BY id
    WITH CUBE
HAVING id IN (
    SELECT id
    FROM test
)
FORMAT `NUll`
SETTINGS enable_analyzer = 1, group_by_use_nulls = true;

SELECT id
FROM test
FINAL
GROUP BY id
    WITH CUBE
HAVING id IN (
    SELECT DISTINCT id
    FROM test
    FINAL
)
FORMAT `NUll`
SETTINGS enable_analyzer = 1, group_by_use_nulls = true;

SELECT id
FROM test
FINAL
GROUP BY
    GROUPING SETS ((id))
ORDER BY
    id IN (
        SELECT DISTINCT id
        FROM test
        FINAL
        LIMIT 4
    ) ASC
LIMIT 256 BY id
FORMAT `NUll`
SETTINGS enable_analyzer = 1, group_by_use_nulls=true;
