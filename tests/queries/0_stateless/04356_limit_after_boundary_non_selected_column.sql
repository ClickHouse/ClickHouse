-- LIMIT AFTER/UNTIL boundary expressions may use columns that are not selected.
-- { echo }

SELECT x FROM (SELECT number AS x, number + 10 AS y FROM numbers(5) ORDER BY x) LIMIT AFTER y >= 13;
SELECT x FROM (SELECT number AS x, number + 10 AS y FROM numbers(5) ORDER BY x) LIMIT AFTER y >= 13 SETTINGS enable_analyzer = 0;

SELECT x FROM (SELECT number AS x, number + 10 AS y FROM numbers(5) ORDER BY x) LIMIT UNTIL y >= 3 + 10;
SELECT x FROM (SELECT number AS x, number + 10 AS y FROM numbers(5) ORDER BY x) LIMIT UNTIL y >= 3 + 10 SETTINGS enable_analyzer = 0;
