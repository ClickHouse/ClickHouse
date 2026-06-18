-- Tags: no-parallel
-- Covers the matcher / qualified-matcher / format-reparse contract edges of `case_insensitive_names = 'standard'`.

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS TestTable;
CREATE TABLE TestTable (FirstName String, LastName String, Age Int32) ENGINE = Memory;
INSERT INTO TestTable VALUES ('Alice', 'A', 25), ('Bob', 'B', 30);

SET case_insensitive_names = 'standard';

SELECT '--- Unqualified COLUMNS(unquoted) matches case-insensitively ---';
SELECT COLUMNS(firstname) FROM TestTable ORDER BY FirstName;
SELECT COLUMNS(FIRSTNAME) FROM TestTable ORDER BY FirstName;

SELECT '--- Unqualified COLUMNS("DoubleQuoted") is case-sensitive ---';
SELECT COLUMNS("FirstName") FROM TestTable ORDER BY FirstName;
-- A column named lowercase `firstname` does not exist; quoted COLUMNS("firstname") must NOT
-- match `FirstName` and the unqualified COLUMNS path now reports an unknown identifier
SELECT COLUMNS("firstname") FROM TestTable; -- { serverError UNKNOWN_IDENTIFIER }

SELECT '--- Unquoted qualified matcher table name ---';
-- Unquoted `testtable.*` resolves against `TestTable` because the qualifier is case-insensitive
SELECT testtable.FirstName FROM TestTable ORDER BY FirstName;

SELECT '--- Quoted qualified matcher table name stays case-sensitive ---';
-- `"TestTable".*` matches the existing exact-case table
SELECT "TestTable".FirstName FROM TestTable ORDER BY FirstName;
-- `"TESTTABLE".*` looks for literal `TESTTABLE`, which does not exist
SELECT "TESTTABLE".FirstName FROM TestTable ORDER BY FirstName; -- { serverError UNKNOWN_IDENTIFIER }

SELECT '--- Quoted alias survives format/reparse ---';
-- `"MyAlias"` should remain case-sensitive after the analyzer formats the AST back and re-parses it
SELECT 1 AS "MyAlias", "MyAlias";
SELECT 1 AS "MyAlias", myalias; -- { serverError UNKNOWN_IDENTIFIER }

SELECT '--- Quoted CTE name survives format/reparse ---';
WITH "MyCte" AS (SELECT 1 AS x) SELECT * FROM "MyCte";
WITH "MyCte" AS (SELECT 1 AS x) SELECT * FROM mycte; -- { serverError UNKNOWN_TABLE }
-- Two distinct quoted CTEs that differ only by case must not collide
WITH "MyCte" AS (SELECT 1 AS x), "mycte" AS (SELECT 2 AS x) SELECT (SELECT x FROM "MyCte") AS a, (SELECT x FROM "mycte") AS b;

DROP TABLE TestTable;
