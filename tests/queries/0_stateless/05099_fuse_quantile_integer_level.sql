-- https://github.com/ClickHouse/ClickHouse/issues/116956
-- Fusing several `quantile` calls into one `quantiles` call must accept a level written as an
-- integer literal, `quantile(0)` or `quantile(1)`, exactly as the aggregate function itself does.

-- The test harness randomizes the setting, so pin it to the value under test.
SET optimize_syntax_fuse_functions = 1;

SELECT quantile(1)(number), quantile(0.5)(number) FROM numbers(100);
SELECT quantile(1)(number), quantile(0.5)(number) FROM numbers(100) SETTINGS optimize_syntax_fuse_functions = 0;

SELECT quantile(0)(number), quantile(1)(number) FROM numbers(100);
SELECT quantile(0)(number), quantile(1)(number) FROM numbers(100) SETTINGS optimize_syntax_fuse_functions = 0;

SELECT quantileExact(1)(number), quantileExact(0.5)(number), quantileExact(0)(number) FROM numbers(100);
SELECT quantileExact(1)(number), quantileExact(0.5)(number), quantileExact(0)(number) FROM numbers(100) SETTINGS optimize_syntax_fuse_functions = 0;

-- A `Decimal` level is converted the same way.
SELECT quantile(toDecimal32(0.5, 2))(number), quantile(1)(number) FROM numbers(100);

-- The fusion still happens: one `quantiles` call feeds both results.
SET enable_analyzer = 1;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT quantile(1)(number), quantile(0.5)(number) FROM numbers(100)) WHERE explain LIKE '%quantiles%';
