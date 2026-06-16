-- https://github.com/ClickHouse/ClickHouse/issues/106956

SELECT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE t1.b = t2.b
UNION ALL
SELECT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE NOT (t1.b = t2.b);

SELECT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE t1.b = t2.b
UNION DISTINCT
SELECT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE NOT (t1.b = t2.b);

SELECT t2.a, CAST(t1.b = t2.b, 'Nullable(UInt8)')
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE t1.b = t2.b
UNION ALL
SELECT t2.a, CAST(t1.b = t2.b, 'Nullable(UInt8)')
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE NOT (t1.b = t2.b);

SELECT DISTINCT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE t1.b = t2.b
UNION ALL
SELECT DISTINCT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE NOT (t1.b = t2.b);

-- The same divergence reaches IntersectOrExceptStep.
SELECT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE t1.b = t2.b
INTERSECT
SELECT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE NOT (t1.b = t2.b);

SELECT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE t1.b = t2.b
EXCEPT
SELECT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE NOT (t1.b = t2.b);

-- Both branches keep rows but fold the same column to different constants (1 and 0).
SELECT a, c FROM (
    SELECT t2.a AS a, (t1.b = t2.b) AS c
    FROM (SELECT 1 AS a, 2 AS b) AS t1
    INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
    WHERE t1.b = t2.b
    UNION ALL
    SELECT t2.a AS a, NOT (t1.b = t2.b) AS c
    FROM (SELECT 1 AS a, 2 AS b) AS t1
    INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
    WHERE t1.b = t2.b
) ORDER BY a, c;

SELECT a, c FROM (
    SELECT t2.a AS a, (t1.b = t2.b) AS c
    FROM (SELECT 1 AS a, 2 AS b) AS t1
    INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
    WHERE t1.b = t2.b
    UNION DISTINCT
    SELECT t2.a AS a, NOT (t1.b = t2.b) AS c
    FROM (SELECT 1 AS a, 2 AS b) AS t1
    INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
    WHERE t1.b = t2.b
) ORDER BY a, c;

SELECT a, c FROM (
    SELECT t2.a AS a, (t1.b = t2.b) AS c
    FROM (SELECT 1 AS a, 2 AS b) AS t1
    INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
    WHERE t1.b = t2.b
    INTERSECT
    SELECT t2.a AS a, NOT (t1.b = t2.b) AS c
    FROM (SELECT 1 AS a, 2 AS b) AS t1
    INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
    WHERE t1.b = t2.b
) ORDER BY a, c;

SELECT a, c FROM (
    SELECT t2.a AS a, (t1.b = t2.b) AS c
    FROM (SELECT 1 AS a, 2 AS b) AS t1
    INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
    WHERE t1.b = t2.b
    EXCEPT
    SELECT t2.a AS a, NOT (t1.b = t2.b) AS c
    FROM (SELECT 1 AS a, 2 AS b) AS t1
    INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
    WHERE t1.b = t2.b
) ORDER BY a, c;
