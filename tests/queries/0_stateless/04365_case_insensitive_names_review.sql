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

SELECT '--- Tuple subcolumn case-insensitive fold ---';
-- Unquoted suffix should match a Tuple subcolumn whose canonical name differs only by case.
WITH CAST(tuple('val'), 'Tuple(Name String)') AS data SELECT data.name;
WITH CAST(tuple('val'), 'Tuple(Name String)') AS data SELECT data."Name";
-- Double-quoted wrong-case suffix stays case-sensitive — must fail.
WITH CAST(tuple('val'), 'Tuple(Name String)') AS data SELECT data."name"; -- { serverError UNKNOWN_IDENTIFIER }

SELECT '--- Temporary table exact-case-first ---';
CREATE TEMPORARY TABLE Temp_review (v Int32);
INSERT INTO Temp_review VALUES (7);
-- Exact-case unquoted lookup binds to the literal temp table.
SELECT v FROM Temp_review;
DROP TEMPORARY TABLE Temp_review;

SELECT '--- EXCEPT/REPLACE transformers (standard mode) ---';
DROP TABLE IF EXISTS t_xform;
CREATE TABLE t_xform (FirstName String, LastName String, Age Int32) ENGINE = Memory;
INSERT INTO t_xform VALUES ('Alice', 'Smith', 30);
-- Unquoted EXCEPT folds case-insensitively; quoted EXCEPT stays case-sensitive.
SELECT * EXCEPT (firstname) FROM t_xform;
SELECT * EXCEPT ("FirstName") FROM t_xform;
-- Unquoted REPLACE folds case-insensitively.
SELECT * REPLACE (0 AS age) FROM t_xform;
-- STRICT EXCEPT/REPLACE: the consumption check still aligns when matched column differs by case.
SELECT * EXCEPT STRICT (firstname) FROM t_xform;
SELECT * REPLACE STRICT (0 AS age) FROM t_xform;

DROP TABLE IF EXISTS t_xform;

SELECT '--- JOIN USING case-insensitive duplicate / matcher qualifier quoting ---';
DROP TABLE IF EXISTS t_jl;
DROP TABLE IF EXISTS t_jr;
CREATE TABLE t_jl (Key Int32, V1 Int32) ENGINE = Memory;
CREATE TABLE t_jr (Key Int32, V2 Int32) ENGINE = Memory;
INSERT INTO t_jl VALUES (1, 10);
INSERT INTO t_jr VALUES (1, 100);
-- USING (Key, key) over column `Key`: both fold to the same canonical key in standard mode → ambiguous.
SELECT * FROM t_jl JOIN t_jr USING (Key, key); -- { serverError BAD_ARGUMENTS }
-- USING (Key, "key") stays distinct (the quoted entry is case-sensitive); but there is no `key` column,
-- so this fails with UNKNOWN_IDENTIFIER instead.
SELECT * FROM t_jl JOIN t_jr USING (Key, "key"); -- { serverError UNKNOWN_IDENTIFIER }
DROP TABLE IF EXISTS t_jl;
DROP TABLE IF EXISTS t_jr;

SELECT '--- information_schema mixed-case view names canonicalize ---';
SELECT count() > 0 FROM information_schema.TaBlEs WHERE TABLE_SCHEMA = 'system';
SELECT count() > 0 FROM INFORMATION_SCHEMA.TaBlEs WHERE TABLE_SCHEMA = 'system';

SELECT '--- AST hash distinguishes quoted vs unquoted CTE output aliases / INTERPOLATE targets ---';
-- Format/reparse must preserve the quote style of CTE output aliases and INTERPOLATE targets so
-- their AST hashes differ — otherwise `QueryResultCache::Key` would collide queries that differ
-- only by that quote bit (and bind targets with different case-sensitivity).
SELECT formatQuery($$ WITH cte(MyCol) AS (SELECT 1) SELECT * FROM cte $$)
    != formatQuery($$ WITH cte("MyCol") AS (SELECT 1) SELECT * FROM cte $$);
-- The quoted CTE alias survives a format round-trip.
SELECT formatQuery($$ WITH cte("MyCol") AS (SELECT 1) SELECT * FROM cte $$) LIKE '%cte("MyCol")%';
-- Same shape for INTERPOLATE targets.
SELECT formatQuery($$ SELECT x FROM (SELECT 1 AS x) ORDER BY x WITH FILL FROM 1 TO 3 INTERPOLATE (x AS x + 1) $$)
    != formatQuery($$ SELECT x FROM (SELECT 1 AS x) ORDER BY x WITH FILL FROM 1 TO 3 INTERPOLATE ("x" AS x + 1) $$);
SELECT formatQuery($$ SELECT x FROM (SELECT 1 AS x) ORDER BY x WITH FILL FROM 1 TO 3 INTERPOLATE ("x" AS x + 1) $$) LIKE '%INTERPOLATE ("x"%';

DROP TABLE IF EXISTS t_using_l;
DROP TABLE IF EXISTS t_using_r;
