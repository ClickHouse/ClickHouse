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

SELECT
    CounterID AS k,
    quantileBFloat16(0.5)(ResolutionWidth)
FROM remote('127.0.0.{1,2}', datasets, hits_v1)
GROUP BY k
ORDER BY
    count() DESC,
    CounterID ASC
LIMIT 10
SETTINGS group_by_use_nulls = 1;
