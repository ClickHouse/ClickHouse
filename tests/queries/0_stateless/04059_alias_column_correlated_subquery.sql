-- Tags: no-old-analyzer, no-replicated-database
-- Regression test for correlated subqueries in ALIAS column expressions.
-- Previously, the server would crash with LOGICAL_ERROR when reading from a table
-- whose ALIAS column expression contained a correlated subquery.
-- The fix converts this to a user-facing NOT_IMPLEMENTED error.
-- The old analyzer resolves ALIAS column references at DDL time, producing
-- UNKNOWN_IDENTIFIER instead of the new analyzer's NOT_IMPLEMENTED path.

DROP TABLE IF EXISTS t_alias_correlated;
CREATE TABLE t_alias_correlated (x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias_correlated SELECT * FROM system.numbers LIMIT 10;

-- Adding an ALIAS column with a nested correlated subquery should succeed
-- (validation only catches top-level subqueries).
ALTER TABLE t_alias_correlated ADD COLUMN s Decimal(2,0) ALIAS toString(intDivOrZero(x, (SELECT max(number) FROM system.numbers WHERE number = x LIMIT 1)));

-- Reading from such a column should return a user-facing error, not crash the server.
SELECT arraySort(groupArray(x)), arraySort(groupArray(s)) FROM t_alias_correlated; -- { serverError NOT_IMPLEMENTED }

-- A simple scalar (non-correlated) subquery in ALIAS should still work fine.
ALTER TABLE t_alias_correlated ADD COLUMN s2 Int64 ALIAS x + (SELECT 1);
SELECT count() FROM t_alias_correlated WHERE s2 > 0;

DROP TABLE t_alias_correlated;
