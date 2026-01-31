SELECT s
FROM
(
    SELECT 'hello4world5' AS s
    UNION ALL
    SELECT 'hello4world10' AS s
    UNION ALL
    SELECT 'hello30' AS s
    UNION ALL
    SELECT 'hello123' AS s
    UNION ALL
    SELECT 'a2' AS s
    UNION ALL
    SELECT 'a10' AS s
    UNION ALL
    SELECT 'a02' AS s
    UNION ALL
    SELECT 'a' AS s
)
ORDER BY s ASC NATURAL;

SELECT s
FROM
(
    SELECT 'hello4world5' AS s
    UNION ALL
    SELECT 'hello4world10' AS s
    UNION ALL
    SELECT 'hello30' AS s
    UNION ALL
    SELECT 'hello123' AS s
    UNION ALL
    SELECT 'a2' AS s
    UNION ALL
    SELECT 'a10' AS s
    UNION ALL
    SELECT 'a02' AS s
    UNION ALL
    SELECT 'a' AS s
)
ORDER BY s DESC NATURAL;