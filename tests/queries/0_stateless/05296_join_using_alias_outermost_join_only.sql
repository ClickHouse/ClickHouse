-- With `analyzer_compatibility_join_using_top_level_identifier` a `JOIN USING` identifier is resolved from an alias of the query.
-- The old analyzer rewrote a query with several JOINs into nested subqueries, so only the outermost JOIN could see such an
-- alias; an inner JOIN took the key from its left table. USING keys at different levels of the join tree follow the same rule.

SET analyzer_compatibility_join_using_top_level_identifier = 1;
SET joined_subquery_requires_alias = 0;

SELECT 'single JOIN: the alias is the key even when the left table has a column with the same name';
SELECT a + 1 AS b, t2.b AS right_b, t2.c FROM (SELECT 1 AS a, 3 AS b) t1 JOIN (SELECT 2 AS b, 7 AS c) t2 USING (b);

SELECT 'inner JOIN of two: the left column is the key';
SELECT a + 1 AS b, t2.b AS right_b, t2.c FROM (SELECT 1 AS a, 3 AS b) t1 JOIN (SELECT 3 AS b, 7 AS c) t2 USING (b) JOIN (SELECT 1 AS z) t3 ON 1 = 1;
-- The alias value would have matched the right side, the left column does not.
SELECT a + 1 AS b, t2.b AS right_b FROM (SELECT 1 AS a, 3 AS b) t1 LEFT JOIN (SELECT 2 AS b) t2 USING (b) JOIN (SELECT 1 AS z) t3 ON 1 = 1;
-- Without a left column the key of an inner JOIN cannot be resolved.
SELECT a + 1 AS b FROM (SELECT 1 AS a) t1 JOIN (SELECT 2 AS b) t2 USING (b) JOIN (SELECT 1 AS z) t3 ON 1 = 1; -- { serverError UNKNOWN_IDENTIFIER }

SELECT 'outermost JOIN of two: the alias is the key';
SELECT a + 1 AS b, t3.b AS right_b, t3.c FROM (SELECT 1 AS a) t1 JOIN (SELECT 1 AS z) t2 ON 1 = 1 JOIN (SELECT 2 AS b, 7 AS c) t3 USING (b);

SELECT 'inner USING JOIN below the aliased outermost USING key';
SELECT a + 1 AS b, t3.b AS right_b, t3.c FROM (SELECT 1 AS a, 10 AS k) t1 JOIN (SELECT 10 AS k) t2 USING (k) JOIN (SELECT 2 AS b, 7 AS c) t3 USING (b);

SELECT 'three JOINs: inner keys from the left tables, the outermost key from the alias';
SELECT k + 100 AS k, m + 100 AS m, a + 1 AS b, t4.b AS right_b, t4.c
FROM (SELECT 1 AS a, 10 AS k) t1
JOIN (SELECT 10 AS k, 20 AS m) t2 USING (k)
JOIN (SELECT 20 AS m) t3 USING (m)
JOIN (SELECT 2 AS b, 7 AS c) t4 USING (b);

SELECT 'WITH alias follows the same rule';
WITH a + 1 AS b SELECT t2.b AS right_b FROM (SELECT 1 AS a, 3 AS b) t1 JOIN (SELECT 3 AS b) t2 USING (b) JOIN (SELECT 1 AS z) t3 ON 1 = 1;
WITH a + 1 AS b SELECT t3.b AS right_b FROM (SELECT 1 AS a, 3 AS b) t1 JOIN (SELECT 1 AS z) t2 ON 1 = 1 JOIN (SELECT 2 AS b) t3 USING (b);

SELECT 'comma JOIN counts as a level';
SELECT a + 1 AS b, t2.b AS right_b FROM (SELECT 1 AS a, 3 AS b) t1 JOIN (SELECT 3 AS b) t2 USING (b), (SELECT 1 AS z) t3;

SELECT 'ARRAY JOIN above the JOIN is not a level: the JOIN stays outermost';
SELECT a + 1 AS b, t2.b AS right_b, x FROM (SELECT 1 AS a, 3 AS b, [1, 2] AS arr) t1 JOIN (SELECT 2 AS b) t2 USING (b) ARRAY JOIN arr AS x ORDER BY x;
-- A JOIN below an ARRAY JOIN that is itself below another JOIN is an inner JOIN.
SELECT a + 1 AS b, t2.b AS right_b, x FROM (SELECT 1 AS a, 3 AS b, [1] AS arr) t1 JOIN (SELECT 3 AS b) t2 USING (b) ARRAY JOIN arr AS x JOIN (SELECT 1 AS z) t3 ON 1 = 1;

SELECT 'a subquery has its own join tree';
SELECT s.b, s.right_b, t.c FROM (SELECT a + 1 AS b, t2.b AS right_b FROM (SELECT 1 AS a, 3 AS b) t1 JOIN (SELECT 2 AS b) t2 USING (b)) s JOIN (SELECT 7 AS c) t ON 1 = 1 JOIN (SELECT 1 AS z) u ON 1 = 1;
SELECT s.b, s.right_b FROM (SELECT a + 1 AS b, t2.b AS right_b FROM (SELECT 1 AS a, 3 AS b) t1 JOIN (SELECT 3 AS b) t2 USING (b) JOIN (SELECT 1 AS z) t3 ON 1 = 1) s;

SELECT 'the reported query: the projection refers to the right side of the inner JOIN';
WITH m AS (SELECT 'a' AS symbol, 'x' AS lp, 1 AS pos),
     b AS (SELECT 'a' AS symbol, 'x' AS lp, 2 AS net_volume),
     dttm AS (SELECT toDateTime('2026-01-01 00:00:00') AS Dttm)
SELECT if(m.symbol = '', b.symbol, m.symbol) AS symbol,
       if(m.lp = '', b.lp, m.lp) AS lp,
       b.net_volume + m.pos AS pos,
       dttm.Dttm AS last_updated
FROM m
FULL OUTER JOIN b USING (symbol, lp)
FULL OUTER JOIN dttm ON 1 = 1;
