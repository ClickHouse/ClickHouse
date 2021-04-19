SELECT *
FROM 
(
    SELECT *
    FROM numbers(100)
    ORDER BY number ASC
)
ORDER BY number ASC
LIMIT 10;

EXPLAIN SYNTAX
SELECT *
FROM 
(
    SELECT *
    FROM numbers(100)
    ORDER BY number ASC
)
ORDER BY number ASC
LIMIT 10;

SELECT *
FROM
(
	SELECT *
    FROM numbers(100)
    ORDER BY number ASC
    UNION ALL
    SELECT *
    FROM numbers(100)
    ORDER BY number ASC
)
ORDER BY number ASC
LIMIT 10;

EXPLAIN SYNTAX
SELECT *
FROM
(
	SELECT *
    FROM numbers(100)
    ORDER BY number ASC
    UNION ALL
    SELECT *
    FROM numbers(100)
    ORDER BY number ASC
)
ORDER BY number ASC
LIMIT 10;

SELECT *
FROM
(
	SELECT *
    FROM numbers(100)
    ORDER BY number ASC
    UNION DISTINCT
    SELECT *
    FROM numbers(100)
    ORDER BY number ASC
)
ORDER BY number ASC
LIMIT 10;

EXPLAIN SYNTAX
SELECT *
FROM
(
	SELECT *
    FROM numbers(100)
    ORDER BY number ASC
    UNION DISTINCT
    SELECT *
    FROM numbers(100)
    ORDER BY number ASC
)
ORDER BY number ASC
LIMIT 10;
