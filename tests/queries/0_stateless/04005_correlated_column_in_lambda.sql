-- Test for correlated columns referenced inside lambda functions
-- Previously, PLACEHOLDER nodes for correlated columns were not properly
-- captured by the lambda mechanism, causing LOGICAL_ERROR during execution.
-- https://github.com/ClickHouse/ClickHouse/issues/85460

SET enable_analyzer = 1;

SELECT (SELECT arrayMap(x -> c0, [1])) FROM (SELECT 1 AS c0);
SELECT (SELECT arrayMap(x -> c0 + x, [1, 2, 3])) FROM (SELECT 10 AS c0);
SELECT (SELECT arrayMap(x -> c0 * x, [1, 2])) FROM (SELECT number AS c0 FROM numbers(3));
