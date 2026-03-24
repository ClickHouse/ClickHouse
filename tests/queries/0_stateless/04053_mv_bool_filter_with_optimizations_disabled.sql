-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/83894
-- Wrong count from materialized view with `query_plan_enable_optimizations = 0`
--
-- When a `MaterializedView` maps an `Int` column to a `Bool` column, a filter on
-- the `Bool` column must not be pushed to the underlying storage using `Bool`
-- semantics as if it were an exact key lookup.  `Bool` `true` means "any non-zero
-- value", so -1 and -2 both satisfy `c0 = TRUE`.

DROP TABLE IF EXISTS t1;
DROP VIEW IF EXISTS v0;

CREATE TABLE t1 (c0 Int8) ENGINE = EmbeddedRocksDB PRIMARY KEY c0;
INSERT INTO t1 VALUES (-1), (-2);

CREATE MATERIALIZED VIEW v0 TO t1 (c0 Bool) AS SELECT c0 FROM t1;

-- With default settings, count should be 2 (both -1 and -2 are truthy as Bool)
SELECT count() FROM v0 WHERE c0 = TRUE;

-- With query_plan_enable_optimizations disabled, the result must be the same
SELECT count() FROM v0 WHERE c0 = TRUE SETTINGS query_plan_enable_optimizations = 0;

DROP VIEW v0;
DROP TABLE t1;
