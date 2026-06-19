-- Regression tests for review blockers on `case_insensitive_names = 'standard'`.

SET allow_experimental_analyzer = 1;
SET case_insensitive_names = 'standard';

SELECT '--- JOIN USING with quoted key (Blocker #6) ---';
DROP TABLE IF EXISTS t_using_l;
DROP TABLE IF EXISTS t_using_r;
CREATE TABLE t_using_l (Key Int32, Val Int32) ENGINE = Memory;
CREATE TABLE t_using_r (Key Int32, Val2 Int32) ENGINE = Memory;
INSERT INTO t_using_l VALUES (1, 10), (2, 20);
INSERT INTO t_using_r VALUES (1, 100), (2, 200);
-- Unquoted USING key — case-insensitive in standard mode.
SELECT count() FROM t_using_l INNER JOIN t_using_r USING (key);
-- Double-quoted USING key — case-sensitive, must match the exact column name.
SELECT count() FROM t_using_l INNER JOIN t_using_r USING ("Key");
SELECT count() FROM t_using_l INNER JOIN t_using_r USING ("key"); -- { serverError UNKNOWN_IDENTIFIER }

SELECT '--- ARRAY JOIN quoted alias (Blocker #7) ---';
-- Quoted alias stays case-sensitive; unquoted lookup must not match it.
SELECT "X" FROM (SELECT 1) ARRAY JOIN [1, 2, 3] AS "X" ORDER BY "X";
SELECT x FROM (SELECT 1) ARRAY JOIN [1, 2, 3] AS "X"; -- { serverError UNKNOWN_IDENTIFIER }
-- Unquoted alias stays case-insensitive.
SELECT y FROM (SELECT 1) ARRAY JOIN [10, 20] AS Y ORDER BY 1;

SELECT '--- Quoted lambda argument (Blocker #5) ---';
SELECT arrayMap(("X") -> "X" + 1, [1, 2, 3]);
SELECT arrayMap(("X") -> x + 1, [1, 2, 3]); -- { serverError UNKNOWN_IDENTIFIER }
SELECT arrayMap((Y) -> y + 1, [1, 2, 3]);

SELECT '--- Recursive CTE with quoted name (Blocker #5) ---';
WITH RECURSIVE "R" AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM "R" WHERE n < 3) SELECT count() FROM "R";
WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT count() FROM r;

SELECT '--- Alias self-reference case awareness (Blocker #4) ---';
-- Case-only-different aliases must coexist (built-in `information_schema` views rely on this).
-- Each alias keeps its own case-sensitive identity, so the projection produces both columns.
SELECT number AS num, num * 1 AS NUM FROM numbers(2) ORDER BY num;

SELECT '--- Quoted alias distinct from unquoted (Blocker #1 hash/cache) ---';
SELECT 1 AS "X", 2 AS x;

SELECT '--- information_schema alias is not ambiguous (Blocker #9) ---';
SELECT count() > 0 FROM information_schema.tables WHERE table_schema = 'system';
SELECT count() > 0 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'system';

DROP TABLE IF EXISTS t_using_l;
DROP TABLE IF EXISTS t_using_r;
