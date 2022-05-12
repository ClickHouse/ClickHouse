SELECT
    number,
    grouping(number, number % 2, number % 3) AS gr
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, gr;

SELECT
    number,
    grouping(number, number % 3, number % 2) AS gr
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, gr;

SELECT
    number,
    grouping(number, number % 2, number % 3) = 2 AS gr
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, gr;

SELECT
    number
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, grouping(number, number % 2, number % 3) = 2;

SELECT
    number,
    count(),
    grouping(number, number % 2, number % 3) AS gr
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number, number % 2),
        ()
    )
ORDER BY (gr, number);

SELECT
    number
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
HAVING grouping(number, number % 2, number % 3) = 4
ORDER BY number
SETTINGS enable_optimize_predicate_expression = 0;

SELECT
    number
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
HAVING grouping(number, number % 2, number % 3) = 2
ORDER BY number
SETTINGS enable_optimize_predicate_expression = 0;
