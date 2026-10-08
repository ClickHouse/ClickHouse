-- Tags: no-parallel

-- Test SQL standard niladic functions without parentheses
SET enable_analyzer = 1;

-- NOW and CURRENT_TIMESTAMP are the same function, so we test both
SELECT toTypeName(NOW);
SELECT toTypeName(CURRENT_TIMESTAMP);
SELECT abs(toInt64(NOW) - toInt64(now())) <= 1;
SELECT abs(toInt64(CURRENT_TIMESTAMP) - toInt64(now())) <= 1;

-- Case insensitivity
SELECT toTypeName(now);
SELECT toTypeName(Now);
SELECT toTypeName(current_timestamp);

-- TODAY and CURRENT_DATE
SELECT toTypeName(TODAY);
SELECT toTypeName(CURRENT_DATE);
SELECT TODAY = today();
SELECT CURRENT_DATE = today();

-- CURRENT_USER
SELECT toTypeName(CURRENT_USER);
SELECT CURRENT_USER = currentUser();

-- -- CURRENT_DATABASE
SELECT toTypeName(CURRENT_DATABASE);
SELECT CURRENT_DATABASE = currentDatabase();

-- Verify they work in expressions
SELECT NOW + INTERVAL 1 DAY > NOW;
SELECT TODAY + 1 > TODAY;

-- Verify they work in WHERE clause
SELECT 1 WHERE NOW > '2000-01-01';
SELECT 1 WHERE TODAY > '2000-01-01';

-- Column name takes precedence over niladic function
CREATE TABLE t (now UInt32) ENGINE = Memory;
INSERT INTO t VALUES (42);
SELECT now FROM t; -- must return 42, not the current timestamp
DROP TABLE t;

-- Parameterized view: niladic function name used as a parameter should NOT resolve as a function
CREATE VIEW test_param_view AS SELECT {currentUser:String} AS value;
SELECT * FROM test_param_view(currentUser = 'currentUser');
SELECT (SELECT * FROM test_param_view(currentUser = (SELECT currentUser))) = currentUser();
DROP VIEW test_param_view;

-- Ensure niladic function names passed as collection keys stay as identifiers
CREATE NAMED COLLECTION IF NOT EXISTS test_niladic_coll AS currentUser = 'admin_user';
SELECT collection FROM system.named_collections WHERE name = 'test_niladic_coll';
DROP NAMED COLLECTION test_niladic_coll;

-- Ensure AS aliases don't interfere with niladic function resolution
WITH 123 as plus
SELECT plus;
WITH TODAY as plus
SELECT plus = TODAY();

-- Test all niladic functions
SELECT DATABASE = currentDatabase();
SELECT SCHEMA = currentDatabase();
SELECT USER = currentUser();
SELECT toTypeName(CURDATE);

-- Test the Default Path resolution
CREATE TABLE t_niladic_default (ts DateTime DEFAULT CURRENT_TIMESTAMP, x UInt8) ENGINE = Memory;
INSERT INTO t_niladic_default (x) VALUES (1);
SELECT toTypeName(ts), ts > '2020-01-01' FROM t_niladic_default;
DROP TABLE t_niladic_default;

-- Test insert select path
CREATE TABLE t_niladic_insert (ts DateTime) ENGINE = Memory;
INSERT INTO t_niladic_insert SELECT CURRENT_TIMESTAMP;
SELECT count() FROM t_niladic_insert WHERE ts > '2020-01-01';
DROP TABLE t_niladic_insert;

-- Verify error for functions that don't allow omitting parentheses
SELECT concat; -- { serverError UNKNOWN_IDENTIFIER }

-- Aggregate functions require parentheses
SELECT count; -- { serverError UNKNOWN_IDENTIFIER }

-- Niladic function in scalar subquery (PR #98941)
SELECT (SELECT DATABASE) = currentDatabase();

-- Niladic function in GROUP BY
SELECT DATABASE = currentDatabase(), count() FROM system.one GROUP BY DATABASE ORDER BY DATABASE;

-- Niladic function in ORDER BY
SELECT 1 AS x ORDER BY NOW;

-- Niladic function in HAVING
SELECT 1 AS x HAVING NOW > '2000-01-01';

-- Niladic function directly inside a function alias body: most targeted test for the PR #98941 fix.
-- Before the fix, allow_niladic_functions was inadvertently false when resolving the FUNCTION-type
-- alias node at QueryAnalyzer.cpp:1084, so TODAY inside toDate(TODAY) would fail to resolve.
WITH toDate(TODAY) AS d SELECT d = today();

-- Niladic in CASE expression
SELECT CASE WHEN DATABASE = currentDatabase() THEN 'ok' ELSE 'fail' END;

-- Niladic in JOIN condition
SELECT a.x FROM (SELECT 1 AS x, DATABASE AS db) a INNER JOIN (SELECT currentDatabase() AS db) b ON a.db = b.db;

-- Multiple niladic functions in single SELECT
SELECT DATABASE = currentDatabase(), USER = currentUser(), TODAY = today();