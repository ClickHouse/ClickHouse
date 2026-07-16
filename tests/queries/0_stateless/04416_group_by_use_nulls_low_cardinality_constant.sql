-- Tags: no-old-analyzer
-- no-old-analyzer: the fix is in `ConstantNode::convertToNullable` (query tree, new analyzer only);
-- the old analyzer does not convert a constant grouping key to Nullable under `group_by_use_nulls`,
-- so it produces a different type for the plain-String key.

-- Tests that a LowCardinality GROUP BY key constant is correctly converted to
-- LowCardinality(Nullable(...)) (not left unchanged) when `group_by_use_nulls` is enabled with
-- GROUPING SETS / ROLLUP / CUBE. Previously the analyzer kept the constant key type as
-- LowCardinality(String), while the runtime produced a LowCardinality(Nullable(String)) column,
-- which led to a `Bad cast from type DB::ColumnNullable to DB::ColumnString` logical error
-- (e.g. when an aggregate function like `minOrDefault` was resolved against the wrong type).
-- https://github.com/ClickHouse/ClickHouse/issues/77485

SET group_by_use_nulls = 1;

-- The declared key type must be LowCardinality(Nullable(String)), matching the runtime column.
SELECT toTypeName(x) FROM (SELECT toLowCardinality('1') AS x GROUP BY GROUPING SETS ((), (x)));

-- Plain String keys were already correct; keep them as a regression guard.
SELECT toTypeName(x) FROM (SELECT '1' AS x GROUP BY GROUPING SETS ((), (x)));

-- Aggregating over such a key with -OrDefault / -OrNull must not produce a logical error.
SELECT minOrDefault(*) FROM (SELECT toLowCardinality('1') GROUP BY GROUPING SETS ((), (1)));
SELECT minOrNull(*) FROM (SELECT toLowCardinality('1') GROUP BY GROUPING SETS ((), (1)));

-- The original fuzzer query that crashed.
SELECT min(c0 >= (SELECT minOrDefault(*) FROM (SELECT DISTINCT toLowCardinality('1') GROUP BY GROUPING SETS ((), (1)) LIMIT 848))) FROM (SELECT 1 AS c0) AS t0;
