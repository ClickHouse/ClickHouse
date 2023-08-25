-- { echoOn }
SELECT number, number % 2, sum(number) AS val
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY ROLLUP(number, number % 2)
ORDER BY (number, number % 2, val)
SETTINGS group_by_use_nulls=1;

SELECT number, number % 2, sum(number) AS val
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY ROLLUP(number, number % 2)
ORDER BY (number, number % 2, val)
SETTINGS group_by_use_nulls=0;

SELECT number, number % 2, sum(number) AS val
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY CUBE(number, number % 2)
ORDER BY (number, number % 2, val)
SETTINGS group_by_use_nulls=1;

SELECT number, number % 2, sum(number) AS val
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY CUBE(number, number % 2)
ORDER BY (number, number % 2, val)
SETTINGS group_by_use_nulls=0;

SELECT
    number,
    number % 2,
    sum(number) AS val
FROM remote('127.0.0.{2,3}', numbers(10))
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
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY (number, number % 2, val)
SETTINGS group_by_use_nulls = 0;

