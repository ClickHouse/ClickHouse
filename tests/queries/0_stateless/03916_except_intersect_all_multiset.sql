-- Test that EXCEPT ALL and INTERSECT ALL correctly handle row multiplicities.
-- https://github.com/ClickHouse/ClickHouse/issues/96801

SELECT '-- EXCEPT ALL: basic (subtract one from two duplicates)';
SELECT 1 AS x
UNION ALL
SELECT 1
EXCEPT ALL
SELECT 1;

SELECT '-- EXCEPT ALL: subtract one occurrence per right-side row';
SELECT * FROM (
    SELECT number FROM numbers(5)
    UNION ALL
    SELECT number FROM numbers(5)
    EXCEPT ALL
    SELECT number FROM numbers(5)
) ORDER BY number;

SELECT '-- EXCEPT ALL: from the issue report';
SELECT 1 AS x UNION ALL SELECT 1 EXCEPT ALL SELECT 1;

SELECT '-- EXCEPT ALL: right side has more duplicates than left';
(SELECT 1 AS x)
EXCEPT ALL
(SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1);

SELECT '-- INTERSECT ALL: min of counts';
(SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 1)
INTERSECT ALL
(SELECT 1 UNION ALL SELECT 1);

SELECT '-- INTERSECT ALL: no common rows';
(SELECT 1 AS x UNION ALL SELECT 1)
INTERSECT ALL
(SELECT 2 UNION ALL SELECT 2);

SELECT '-- EXCEPT ALL: empty right side';
SELECT * FROM (
    SELECT number FROM numbers(3)
    EXCEPT ALL
    SELECT number FROM numbers(0)
) ORDER BY number;

SELECT '-- EXCEPT ALL: empty left side';
SELECT number FROM numbers(0)
EXCEPT ALL
SELECT number FROM numbers(3);

SELECT '-- INTERSECT ALL: empty right side';
SELECT number FROM numbers(3)
INTERSECT ALL
SELECT number FROM numbers(0);

SELECT '-- EXCEPT ALL with strings';
SELECT * FROM (
    (SELECT 'a' AS x UNION ALL SELECT 'a' UNION ALL SELECT 'b')
    EXCEPT ALL
    (SELECT 'a')
) ORDER BY x;

SELECT '-- INTERSECT ALL with strings';
SELECT * FROM (
    (SELECT 'a' AS x UNION ALL SELECT 'a' UNION ALL SELECT 'b')
    INTERSECT ALL
    (SELECT 'a' UNION ALL SELECT 'b' UNION ALL SELECT 'b')
) ORDER BY x;

SELECT '-- EXCEPT DISTINCT still works (removes all matching)';
(SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2)
EXCEPT DISTINCT
(SELECT 1);

SELECT '-- INTERSECT DISTINCT still works';
(SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2)
INTERSECT DISTINCT
(SELECT 1 UNION ALL SELECT 3);
