-- Tags: no-parallel
-- SQL Standard: unquoted = case-insensitive, double-quoted = case-sensitive, backtick-quoted = case-insensitive

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS test_quote;
CREATE TABLE test_quote (MyColumn Int32) ENGINE = Memory;
INSERT INTO test_quote VALUES (42);

SET case_insensitive_names = 'standard';
SELECT '--- Unquoted vs double-quoted ---';

-- Unquoted: case-insensitive
SELECT mycolumn FROM test_quote;
SELECT MYCOLUMN FROM test_quote;
SELECT MyColumn FROM test_quote;

-- Double-quoted: case-sensitive (exact match only)
SELECT "MyColumn" FROM test_quote;
SELECT "mycolumn" FROM test_quote; -- { serverError UNKNOWN_IDENTIFIER }
SELECT "MYCOLUMN" FROM test_quote; -- { serverError UNKNOWN_IDENTIFIER }

SET case_insensitive_names = 'standard';
SELECT '--- Backtick-quoted (case-insensitive) ---';

SELECT `MyColumn` FROM test_quote;
SELECT `mycolumn` FROM test_quote;
SELECT `MYCOLUMN` FROM test_quote;

DROP TABLE IF EXISTS test_ambig;
CREATE TABLE test_ambig (Val Int32, val Int32) ENGINE = Memory;
INSERT INTO test_ambig VALUES (100, 200);

SET case_insensitive_names = 'standard';
SELECT '--- Disambiguating with quotes ---';

-- Unquoted: ambiguous
SELECT Val FROM test_ambig; -- { serverError AMBIGUOUS_IDENTIFIER }
SELECT val FROM test_ambig; -- { serverError AMBIGUOUS_IDENTIFIER }

-- Double-quoted: exact match, no ambiguity
SELECT "Val" FROM test_ambig;
SELECT "val" FROM test_ambig;

-- Wrong case with double-quote: should fail
SELECT "VAL" FROM test_ambig; -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE IF EXISTS test_three;
CREATE TABLE test_three (ABC Int32, Abc Int32, abc Int32) ENGINE = Memory;
INSERT INTO test_three VALUES (1, 2, 3);

SET case_insensitive_names = 'standard';
SELECT '--- Three-way ambiguity ---';

-- Unquoted: all ambiguous
SELECT abc FROM test_three; -- { serverError AMBIGUOUS_IDENTIFIER }
SELECT ABC FROM test_three; -- { serverError AMBIGUOUS_IDENTIFIER }

-- Each double-quoted variant accesses its specific column
SELECT "ABC" FROM test_three;
SELECT "Abc" FROM test_three;
SELECT "abc" FROM test_three;

DROP TABLE IF EXISTS test_mixed;
CREATE TABLE test_mixed (FirstName String, lastName String) ENGINE = Memory;
INSERT INTO test_mixed VALUES ('John', 'Doe');

SET case_insensitive_names = 'standard';
SELECT '--- Mixed quoting ---';

-- Mix of unquoted (case-insensitive) and quoted (case-sensitive)
SELECT firstname, "lastName" FROM test_mixed;
SELECT "FirstName", lastname FROM test_mixed;

-- Wrong case in double-quoted should fail
SELECT firstname, "LASTNAME" FROM test_mixed; -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE IF EXISTS test_compound;
CREATE TABLE test_compound (Col Int32) ENGINE = Memory;
INSERT INTO test_compound VALUES (888);

SET case_insensitive_names = 'standard';
SELECT '--- Compound identifiers with quoting ---';

-- table.column - both unquoted (both case-insensitive)
SELECT test_compound.col FROM test_compound;
SELECT TEST_COMPOUND.COL FROM test_compound;

-- table."Column" - column double-quoted (column case-sensitive)
SELECT test_compound."Col" FROM test_compound;
SELECT test_compound."col" FROM test_compound; -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE IF EXISTS test_alias;
CREATE TABLE test_alias (col Int32) ENGINE = Memory;
INSERT INTO test_alias VALUES (123);

SET case_insensitive_names = 'standard';
SELECT '--- Alias quoting ---';

-- Unquoted alias: case-insensitive reference
SELECT col AS MyAlias FROM test_alias;
SELECT myalias FROM (SELECT col AS MyAlias FROM test_alias);
SELECT MYALIAS FROM (SELECT col AS MyAlias FROM test_alias);

DROP TABLE IF EXISTS test_special;
CREATE TABLE test_special (`Column With Spaces` Int32, `column-with-dashes` Int32) ENGINE = Memory;
INSERT INTO test_special VALUES (111, 222);

SET case_insensitive_names = 'standard';
SELECT '--- Special characters in identifiers ---';

SELECT `Column With Spaces` FROM test_special;
SELECT `column-with-dashes` FROM test_special;

-- Case sensitivity still applies for backticks
SELECT `column with spaces` FROM test_special;
SELECT `COLUMN-WITH-DASHES` FROM test_special;

DROP TABLE IF EXISTS test_default;
CREATE TABLE test_default (MyCol Int32) ENGINE = Memory;
INSERT INTO test_default VALUES (99);

SET case_insensitive_names = 'default';
SELECT '--- Default mode ---';

SELECT MyCol FROM test_default;
SELECT mycol FROM test_default; -- { serverError UNKNOWN_IDENTIFIER }
SELECT "MyCol" FROM test_default;
SELECT "mycol" FROM test_default; -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE IF EXISTS test_quote;
DROP TABLE IF EXISTS test_ambig;
DROP TABLE IF EXISTS test_three;
DROP TABLE IF EXISTS test_mixed;
DROP TABLE IF EXISTS test_compound;
DROP TABLE IF EXISTS test_alias;
DROP TABLE IF EXISTS test_special;
DROP TABLE IF EXISTS test_default;

