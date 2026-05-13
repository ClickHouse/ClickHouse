-- Regression test: filter push-down must not introduce a Const/non-Const header
-- mismatch when the remaining (non-pushed) expression folds to a constant due to
-- a NULL argument (defaultImplementationForNulls short-circuit).
-- https://github.com/ClickHouse/ClickHouse/issues/XXXXX

SELECT dummy AND count() + NULL AS h FROM system.one GROUP BY dummy HAVING h UNION ALL SELECT 1;
