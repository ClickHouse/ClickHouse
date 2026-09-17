-- Repro for https://github.com/ClickHouse/ClickHouse/issues/89062
SELECT 1
FROM numbers(1) AS t0
WHERE EXISTS (SELECT t0._table)
SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, allow_experimental_correlated_subqueries = 1, enable_analyzer = 1;
