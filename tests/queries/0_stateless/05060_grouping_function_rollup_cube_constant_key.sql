-- A constant GROUP BY key participates in ROLLUP and CUBE like any other key,
-- so it adds its own grouping levels and its own bit to the GROUPING mask.

SET enable_analyzer = 1;

SELECT 'GROUPING SETS with a constant key';

SELECT
    person,
    'x' AS cv,
    count() AS c,
    GROUPING(person, cv) AS g
FROM VALUES('person String', 'Noah', 'Emma') AS p
GROUP BY GROUPING SETS ((person, cv), ())
ORDER BY g, person;

SELECT 'ROLLUP with a constant key';

SELECT
    count() AS c,
    1 AS k,
    number,
    GROUPING(k, number) AS g
FROM numbers(3)
GROUP BY ROLLUP(k, number)
ORDER BY g, number
SETTINGS enable_positional_arguments = 0;

SELECT 'CUBE with a constant key';

SELECT
    count() AS c,
    1 AS k,
    number,
    GROUPING(k, number) AS g
FROM numbers(3)
GROUP BY CUBE(k, number)
ORDER BY g, number
SETTINGS enable_positional_arguments = 0;

SELECT 'ROLLUP with a constant key and no GROUPING';

SELECT count() AS c
FROM numbers(3)
GROUP BY ROLLUP(1, number)
ORDER BY c
SETTINGS enable_positional_arguments = 0;
