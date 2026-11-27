-- Tags: no-replicated-database
-- Regression test for subqueries in ALIAS/DEFAULT column expressions.
-- Previously, the server would crash with LOGICAL_ERROR when reading from a table
-- whose ALIAS column expression contained a correlated subquery nested inside
-- function arguments, because ColumnsDescription validation only checked
-- direct children for subquery nodes.
-- The fix makes subquery detection recursive so that CREATE/ALTER TABLE rejects
-- such expressions at DDL time rather than persisting invalid schema.

DROP TABLE IF EXISTS t_alias_correlated;
CREATE TABLE t_alias_correlated (x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias_correlated SELECT * FROM system.numbers LIMIT 10;

-- A nested correlated subquery inside an ALIAS function argument must be rejected
-- at DDL time (THERE_IS_NO_DEFAULT_VALUE), not silently accepted.
ALTER TABLE t_alias_correlated ADD COLUMN s Decimal(2,0) ALIAS toString(intDivOrZero(x, (SELECT max(number) FROM system.numbers WHERE number = x LIMIT 1))); -- { serverError THERE_IS_NO_DEFAULT_VALUE }

-- A nested non-correlated subquery is also rejected at DDL time for consistency
-- (subqueries at any nesting depth are not supported in column defaults).
ALTER TABLE t_alias_correlated ADD COLUMN s2 Int64 ALIAS x + (SELECT 1); -- { serverError THERE_IS_NO_DEFAULT_VALUE }

-- A top-level subquery as DEFAULT was already rejected before this fix.
ALTER TABLE t_alias_correlated ADD COLUMN s3 Int64 DEFAULT (SELECT 1); -- { serverError THERE_IS_NO_DEFAULT_VALUE }

-- Verify the table still works normally (no columns with subqueries were added).
SELECT count() FROM t_alias_correlated;

DROP TABLE t_alias_correlated;
