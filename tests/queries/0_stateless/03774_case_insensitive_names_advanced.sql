-- Tags: no-parallel
-- Tests: ambiguity, joins, subqueries, CTEs, aliases, nested columns, edge cases

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS test_ambig;
CREATE TABLE test_ambig (Score Int32, sCOrE Int32) ENGINE = Memory;
INSERT INTO test_ambig VALUES (10, 20);

SET case_insensitive_names = 'standard';
SELECT '--- Column ambiguity ---';

-- All variants are ambiguous
SELECT Score FROM test_ambig; -- { serverError AMBIGUOUS_IDENTIFIER }
SELECT sCOrE FROM test_ambig; -- { serverError AMBIGUOUS_IDENTIFIER }
SELECT score FROM test_ambig; -- { serverError AMBIGUOUS_IDENTIFIER }
SELECT SCORE FROM test_ambig; -- { serverError AMBIGUOUS_IDENTIFIER }

DROP TABLE IF EXISTS test_mix;
CREATE TABLE test_mix (Id Int32, ID Int32, Name String) ENGINE = Memory;
INSERT INTO test_mix VALUES (1, 2, 'Test');

SET case_insensitive_names = 'standard';
SELECT '--- Mixed ambiguity ---';

-- Name is unambiguous
SELECT name FROM test_mix;
SELECT NAME FROM test_mix;

-- Id/ID are ambiguous
SELECT id FROM test_mix; -- { serverError AMBIGUOUS_IDENTIFIER }

DROP TABLE IF EXISTS test_j1;
DROP TABLE IF EXISTS test_j2;

CREATE TABLE test_j1 (ID UInt8, Val UInt8) ENGINE = Memory;
CREATE TABLE test_j2 (Num UInt8, Amount UInt8) ENGINE = Memory;
INSERT INTO test_j1 VALUES (1, 10), (2, 20);
INSERT INTO test_j2 VALUES (1, 100), (2, 200);

SET case_insensitive_names = 'standard';
SELECT '--- JOIN basic ---';

SELECT j1.val, j2.amount FROM test_j1 AS j1 INNER JOIN test_j2 AS j2 ON j1.id = j2.num ORDER BY j1.val;
SELECT j1.VAL, j2.AMOUNT FROM test_j1 AS j1 INNER JOIN test_j2 AS j2 ON j1.ID = j2.NUM ORDER BY j1.VAL;

SET case_insensitive_names = 'standard';
SELECT '--- Self-join ---';

SELECT a.val, b.val FROM test_j1 AS a INNER JOIN test_j1 AS b ON a.ID = b.id WHERE a.id = 1 AND b.id = 2;

DROP TABLE IF EXISTS test_sub;
CREATE TABLE test_sub (Value Int32) ENGINE = Memory;
INSERT INTO test_sub VALUES (10), (20), (30);

SET case_insensitive_names = 'standard';
SELECT '--- Subqueries ---';

SELECT myval FROM (SELECT Value AS MyVal FROM test_sub) ORDER BY MYVAL;
SELECT MYVAL FROM (SELECT Value AS myval FROM test_sub) ORDER BY myval;

-- Nested subqueries
SELECT inner_val FROM (
    SELECT outer_val AS inner_val FROM (
        SELECT Value AS outer_val FROM test_sub
    )
) ORDER BY INNER_VAL;

SET case_insensitive_names = 'standard';
SELECT '--- CTE ---';

WITH MyCTE AS (SELECT Value FROM test_sub)
SELECT * FROM MyCTE ORDER BY value;

WITH cte AS (SELECT Value AS MyVal FROM test_sub)
SELECT myval FROM cte ORDER BY MYVAL;

DROP TABLE IF EXISTS test_alias;
CREATE TABLE test_alias (Value Int32, Other Int32) ENGINE = Memory;
INSERT INTO test_alias VALUES (10, 20), (30, 40);

SET case_insensitive_names = 'standard';
SELECT '--- Column aliases ---';

-- Alias access with different case
SELECT Value AS MyValue FROM test_alias ORDER BY myvalue;
SELECT Value AS MyValue FROM test_alias ORDER BY MYVALUE;

-- Alias in expressions
SELECT Value * 2 AS DoubleVal, doubleVAL + 1 AS PlusOne FROM test_alias ORDER BY plusone;

-- Alias in WHERE (ClickHouse extension)
SELECT Value AS V FROM test_alias WHERE v > 15 ORDER BY V;

SET case_insensitive_names = 'standard';
SELECT '--- Table aliases ---';

SELECT t.value FROM test_alias AS T ORDER BY t.value;
SELECT T.Value FROM test_alias AS t ORDER BY T.Value;

-- Cross-join with aliases
SELECT a.value, B.value 
FROM test_alias AS A 
CROSS JOIN test_alias AS B 
WHERE a.value = 10 AND b.value = 30;

DROP TABLE IF EXISTS test_nested;
CREATE TABLE test_nested (Data Tuple(Name String, Age UInt8)) ENGINE = Memory;
INSERT INTO test_nested VALUES (('Alice', 25));

SET case_insensitive_names = 'standard';
SELECT '--- Nested columns ---';

SELECT data.Name FROM test_nested;
SELECT data.name FROM test_nested;
SELECT DATA.NAME FROM test_nested;

DROP TABLE IF EXISTS test_having;
CREATE TABLE test_having (Category String, Amount Int32) ENGINE = Memory;
INSERT INTO test_having VALUES ('A', 10), ('A', 20), ('B', 15), ('B', 25);

SET case_insensitive_names = 'standard';
SELECT '--- HAVING with aliases ---';

SELECT Category, sum(Amount) AS Total FROM test_having GROUP BY Category HAVING total > 20 ORDER BY Category;
SELECT Category, sum(Amount) AS Total FROM test_having GROUP BY Category HAVING TOTAL > 20 ORDER BY Category;

DROP TABLE IF EXISTS test_empty;
CREATE TABLE test_empty (Column1 Int32, COLUMN1 Int32) ENGINE = Memory;

SET case_insensitive_names = 'standard';
SELECT '--- Empty table ambiguity ---';

-- Ambiguity detected even with empty table
SELECT column1 FROM test_empty; -- { serverError AMBIGUOUS_IDENTIFIER }

DROP TABLE IF EXISTS test_lambda;
CREATE TABLE test_lambda (Arr Array(Int32)) ENGINE = Memory;
INSERT INTO test_lambda VALUES ([1, 2, 3]), ([4, 5, 6]);

SET case_insensitive_names = 'standard';
SELECT '--- Lambda expressions ---';

SELECT arrayMap(X -> X * 2, Arr) FROM test_lambda ORDER BY Arr;

SET case_insensitive_names = 'default';
SELECT '--- Default mode JOIN ---';

SELECT j1.Val, j2.Amount FROM test_j1 AS j1 INNER JOIN test_j2 AS j2 ON j1.ID = j2.Num ORDER BY j1.Val;
SELECT j1.val FROM test_j1 AS j1 INNER JOIN test_j2 AS j2 ON j1.id = j2.num; -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE IF EXISTS test_ambig;
DROP TABLE IF EXISTS test_mix;
DROP TABLE IF EXISTS test_j1;
DROP TABLE IF EXISTS test_j2;
DROP TABLE IF EXISTS test_sub;
DROP TABLE IF EXISTS test_alias;
DROP TABLE IF EXISTS test_nested;
DROP TABLE IF EXISTS test_having;
DROP TABLE IF EXISTS test_empty;
DROP TABLE IF EXISTS test_lambda;

