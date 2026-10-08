-- Test for https://github.com/ClickHouse/ClickHouse/issues/14004
-- SHOW CREATE TABLE and DESCRIBE TABLE should be consistent with respect to TEMPORARY tables.

SET describe_compact_output = 1;

DROP TABLE IF EXISTS t;

CREATE TABLE t (hello String) ENGINE = MergeTree ORDER BY hello;
CREATE TEMPORARY TABLE t (world UInt64);

-- Both SHOW CREATE TABLE and DESCRIBE TABLE should prefer the temporary table
-- when no database is specified.
SELECT 'SHOW CREATE TABLE (no database qualifier) should show temporary table:';
SHOW CREATE TABLE t;

SELECT 'DESCRIBE TABLE (no database qualifier) should show temporary table:';
DESCRIBE TABLE t;

-- DESCRIBE TEMPORARY TABLE should work (was a syntax error before the fix).
SELECT 'DESCRIBE TEMPORARY TABLE:';
DESCRIBE TEMPORARY TABLE t;

-- SHOW CREATE TEMPORARY TABLE should still work.
SELECT 'SHOW CREATE TEMPORARY TABLE:';
SHOW CREATE TEMPORARY TABLE t;

-- DESCRIBE TEMPORARY TABLE with subquery or table function should be a syntax error.
DESCRIBE TEMPORARY TABLE (SELECT 1); -- { clientError SYNTAX_ERROR }
DESCRIBE TEMPORARY TABLE SELECT 1; -- { clientError SYNTAX_ERROR }
DESCRIBE TEMPORARY TABLE numbers(10); -- { clientError SYNTAX_ERROR }

-- SHOW CREATE VIEW should still resolve a permanent view when a temporary table with the
-- same name exists. Without this, SHOW CREATE VIEW would resolve to the temporary table and
-- throw BAD_ARGUMENTS ("... is not a VIEW"). Use FORMAT Null to avoid printing the database
-- name, which differs between test runs.
DROP VIEW IF EXISTS v;
DROP TABLE IF EXISTS v;
CREATE TEMPORARY TABLE v (x UInt8);
CREATE VIEW v AS SELECT 1 AS y;

SELECT 'SHOW CREATE VIEW with same-named temporary table succeeds:';
SHOW CREATE VIEW v FORMAT Null;
SELECT 'OK';

-- Regression test: a table named `temporary` should still work with DESCRIBE.
DROP TABLE IF EXISTS temporary;
CREATE TABLE temporary (x UInt8) ENGINE = MergeTree ORDER BY x;

SELECT 'DESCRIBE unquoted table named temporary:';
DESCRIBE temporary;

DROP TABLE temporary;

DROP TEMPORARY TABLE t;
DROP TABLE t;
