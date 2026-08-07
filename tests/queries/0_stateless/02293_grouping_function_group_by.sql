set optimize_group_by_function_keys=0;

SELECT
    number,
    grouping(number, number % 2, number % 3) = 6
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    number,
    number % 2
ORDER BY number; -- { serverError BAD_ARGUMENTS }

-- { echoOn }
SELECT
    number,
    grouping(number, number % 2) = 3
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    number,
    number % 2
ORDER BY number
SETTINGS force_grouping_standard_compatibility=0;

SELECT
    number,
    grouping(number),
    GROUPING(number % 2)
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    number,
    number % 2
ORDER BY number
SETTINGS force_grouping_standard_compatibility=0;

SELECT
    number,
    grouping(number, number % 2) AS gr
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    number,
    number % 2
    WITH ROLLUP
ORDER BY
    number, gr
SETTINGS force_grouping_standard_compatibility=0;

SELECT
    number,
    grouping(number, number % 2) AS gr
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    ROLLUP(number, number % 2)
ORDER BY
    number, gr
SETTINGS force_grouping_standard_compatibility=0;

SELECT
    number,
    grouping(number, number % 2) AS gr
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    number,
    number % 2
    WITH CUBE
ORDER BY
    number, gr
SETTINGS force_grouping_standard_compatibility=0;

SELECT
    number,
    grouping(number, number % 2) AS gr
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    CUBE(number, number % 2)
ORDER BY
    number, gr
SETTINGS force_grouping_standard_compatibility=0;

SELECT
    number,
    grouping(number, number % 2) + 3 as gr
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    CUBE(number, number % 2)
HAVING grouping(number) != 0
ORDER BY
    number, gr
SETTINGS force_grouping_standard_compatibility=0;

SELECT
    number,
    grouping(number, number % 2) as gr
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    CUBE(number, number % 2) WITH TOTALS
HAVING grouping(number) != 0
ORDER BY
    number, gr; -- { serverError NOT_IMPLEMENTED }

SELECT
    number,
    grouping(number, number % 2) as gr
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    CUBE(number, number % 2) WITH TOTALS
ORDER BY
    number, gr
SETTINGS force_grouping_standard_compatibility=0;

SELECT
    number,
    grouping(number, number % 2) as gr
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    ROLLUP(number, number % 2) WITH TOTALS
HAVING grouping(number) != 0
ORDER BY
    number, gr; -- { serverError NOT_IMPLEMENTED }

SELECT
    number,
    grouping(number, number % 2) as gr
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    ROLLUP(number, number % 2) WITH TOTALS
ORDER BY
    number, gr
SETTINGS force_grouping_standard_compatibility=0;
