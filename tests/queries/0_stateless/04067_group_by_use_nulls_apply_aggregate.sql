-- Regression test for crash: Bad cast from ColumnVector to ColumnNullable
-- when using APPLY with aggregate functions and group_by_use_nulls=1.
-- The Analyzer converted matched GROUP BY key columns to Nullable before
-- passing them to APPLY, but aggregate function arguments should use
-- pre-aggregation (non-Nullable) types. The Nullable wrapping is handled
-- post-aggregation by Rollup/Cube/GroupingSets transforms.
-- https://github.com/ClickHouse/ClickHouse/issues/100450

SET group_by_use_nulls = 1;

-- Original crash: GROUPING SETS with APPLY lambda + aggregate
SELECT * APPLY x -> argMax(x, number) FROM numbers(1) GROUP BY GROUPING SETS ((materialize(65537)), (*));

-- ROLLUP variant
SELECT * APPLY x -> argMax(x, number) FROM numbers(1) GROUP BY number WITH ROLLUP;

-- CUBE variant
SELECT * APPLY x -> argMax(x, number) FROM numbers(1) GROUP BY number WITH CUBE;

-- Multiple columns
SELECT * APPLY x -> argMax(x, b) FROM (SELECT number AS a, number * 2 AS b FROM numbers(3)) GROUP BY a, b WITH ROLLUP ORDER BY a, b;

-- sum aggregate (another common aggregate function)
SELECT * APPLY x -> sum(x) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY number;

-- Nested aggregate inside another function (must also skip Nullable conversion)
SELECT * APPLY x -> toString(argMax(x, number)) FROM numbers(1) GROUP BY GROUPING SETS ((materialize(65537)), (*));

-- Deeply nested aggregate
SELECT * APPLY x -> length(toString(sum(x))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY number;

-- Function-form aggregate (covers ApplyColumnTransformerType::FUNCTION branch with aggregate detection)
SELECT * APPLY sum FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY number;

-- Non-aggregate APPLY should still produce Nullable output
SELECT * APPLY toString FROM (SELECT number FROM numbers(2)) GROUP BY number WITH ROLLUP ORDER BY number;
