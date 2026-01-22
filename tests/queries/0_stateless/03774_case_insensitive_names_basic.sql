-- Tags: no-parallel
-- Tests: standard mode, default mode, column resolution, compound identifiers

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS test_basic;
CREATE TABLE test_basic (FirstName String, lastName String, AGE UInt8) ENGINE = Memory;
INSERT INTO test_basic VALUES ('John', 'Doe', 30);

SET case_insensitive_names = 'standard';
SELECT '--- Standard mode: column case-insensitivity ---';

SELECT FirstName FROM test_basic;
SELECT firstname FROM test_basic;
SELECT FIRSTNAME FROM test_basic;
SELECT lastname FROM test_basic;
SELECT LASTNAME FROM test_basic;
SELECT age FROM test_basic;
SELECT Age FROM test_basic;

SET case_insensitive_names = 'default';
SELECT '--- Default mode: case-sensitive ---';

SELECT FirstName FROM test_basic;
SELECT firstname FROM test_basic; -- { serverError UNKNOWN_IDENTIFIER }
SELECT LASTNAME FROM test_basic; -- { serverError UNKNOWN_IDENTIFIER }

SET case_insensitive_names = 'standard';
SELECT '--- Compound identifiers ---';

SELECT test_basic.firstname FROM test_basic;
SELECT test_basic.FIRSTNAME FROM test_basic;
SELECT TEST_BASIC.firstname FROM test_basic;
SELECT TEST_BASIC.FIRSTNAME FROM test_basic;

SET case_insensitive_names = 'standard';
SELECT '--- Table alias case-insensitivity ---';

SELECT t.firstname FROM test_basic AS t;
SELECT T.firstname FROM test_basic AS t;
SELECT T.FIRSTNAME FROM test_basic AS t;

SET case_insensitive_names = 'standard';
SELECT '--- Expressions ---';

SELECT upper(firstname), age + 5 FROM test_basic;
SELECT UPPER(FIRSTNAME), AGE + 5 FROM test_basic;

DROP TABLE IF EXISTS test_order;
CREATE TABLE test_order (Name String, Value Int32) ENGINE = Memory;
INSERT INTO test_order VALUES ('B', 2), ('A', 1), ('C', 3);

SET case_insensitive_names = 'standard';
SELECT '--- ORDER BY ---';

SELECT name, value FROM test_order ORDER BY name;
SELECT NAME, VALUE FROM test_order ORDER BY NAME;

DROP TABLE IF EXISTS test_group;
CREATE TABLE test_group (Category String, Amount Int32) ENGINE = Memory;
INSERT INTO test_group VALUES ('A', 10), ('A', 20), ('B', 15);

SET case_insensitive_names = 'standard';
SELECT '--- GROUP BY ---';

SELECT category, sum(amount) FROM test_group GROUP BY category ORDER BY category;
SELECT CATEGORY, SUM(AMOUNT) FROM test_group GROUP BY CATEGORY ORDER BY CATEGORY;

SET case_insensitive_names = 'standard';
SELECT '--- DISTINCT ---';

SELECT DISTINCT category FROM test_group ORDER BY category;
SELECT DISTINCT CATEGORY FROM test_group ORDER BY CATEGORY;

DROP TABLE IF EXISTS test_basic;
DROP TABLE IF EXISTS test_order;
DROP TABLE IF EXISTS test_group;
