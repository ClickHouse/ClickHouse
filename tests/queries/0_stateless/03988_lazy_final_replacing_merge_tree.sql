-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_lazy_final;

CREATE TABLE t_lazy_final
(
    timestamp DateTime,
    id UInt64,
    version UInt64,
    status String,
    value Float64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (toStartOfDay(timestamp), id)
SETTINGS index_granularity = 256;

-- Insert data in multiple parts.
INSERT INTO t_lazy_final SELECT
    toDateTime('2024-01-01 00:00:00') + number * 60,
    number % 1000,
    1,
    if(number % 3 = 0, 'active', 'inactive'),
    number * 1.5
FROM numbers(5000);

INSERT INTO t_lazy_final SELECT
    toDateTime('2024-01-01 00:00:00') + number * 60,
    number % 1000,
    2,
    if(number % 3 = 0, 'deleted', 'active'),
    number * 2.0
FROM numbers(3000);

INSERT INTO t_lazy_final SELECT
    toDateTime('2024-01-05 00:00:00') + number * 60,
    number % 500,
    3,
    'active',
    number * 3.0
FROM numbers(2000);

-- Verify results match with and without optimization.

SELECT '-- filter on status';
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active'
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active'
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000;

SELECT '-- filter on timestamp range';
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE toStartOfDay(timestamp) = '2024-01-01'
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE toStartOfDay(timestamp) = '2024-01-01'
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000;

SELECT '-- combined filter';
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active' AND id < 100
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active' AND id < 100
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000;

-- Fallback: set row limit so small that the set will be truncated. Results must still be correct.
SELECT '-- small row limit (fallback)';
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active'
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10;

-- Fallback: set byte limit very small.
SELECT '-- small byte limit (fallback)';
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active'
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, max_bytes_for_lazy_final = 100;

-- PREWHERE.
SELECT '-- prewhere';
SELECT count(), sum(value) FROM t_lazy_final FINAL PREWHERE id < 200 WHERE status = 'active'
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT count(), sum(value) FROM t_lazy_final FINAL PREWHERE id < 200 WHERE status = 'active'
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000;

-- Row policy.
DROP ROW POLICY IF EXISTS policy_lazy_final ON t_lazy_final;
CREATE ROW POLICY policy_lazy_final ON t_lazy_final FOR SELECT USING id < 500 TO ALL;

SELECT '-- row policy';
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active'
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active'
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000;

DROP ROW POLICY policy_lazy_final ON t_lazy_final;

-- Plan checks: optimization enabled with filter should have InputSelector.
SELECT '-- plan: optimization has InputSelector';
SELECT explain LIKE '%InputSelector%' FROM (
    EXPLAIN actions = 0
    SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000
) WHERE explain LIKE '%InputSelector%';

-- Plan checks: optimization disabled should not have InputSelector.
SELECT '-- plan: no optimization has no InputSelector';
SELECT count() FROM (
    EXPLAIN actions = 0
    SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 0
) WHERE explain LIKE '%InputSelector%';

-- Plan checks: no filter means optimization should not apply.
SELECT '-- plan: no filter has no InputSelector';
SELECT count() FROM (
    EXPLAIN actions = 0
    SELECT count(), sum(value) FROM t_lazy_final FINAL
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000
) WHERE explain LIKE '%InputSelector%';

DROP TABLE t_lazy_final;
