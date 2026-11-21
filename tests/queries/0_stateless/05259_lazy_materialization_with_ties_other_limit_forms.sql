-- Tags: no-random-settings, no-parallel-replicas
-- `query_plan_optimize_lazy_materialization_with_ties` applies only to a plain `LIMIT n [OFFSET m] WITH TIES`
-- handled by `LimitStep`. Fractional limits and fractional or negative offsets are planned with other steps
-- (`FractionalLimitStep`, `FractionalOffsetStep`, `NegativeOffsetStep`) and are not lazily materialized.

DROP TABLE IF EXISTS test_lazy_materialization_with_ties_forms;

CREATE TABLE test_lazy_materialization_with_ties_forms
(
    k UInt64,
    tie UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO test_lazy_materialization_with_ties_forms
SELECT number, intDiv(number, 5), repeat('payload', 16)
FROM numbers(20);

SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_optimize_lazy_materialization_with_ties = 1;
SET query_plan_max_limit_for_lazy_materialization = 10;

SELECT 'plain limit (control)';
SELECT max(explain LIKE '%Lazily read columns:%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT k, payload
    FROM test_lazy_materialization_with_ties_forms
    ORDER BY tie
    LIMIT 3 WITH TIES
);

SELECT 'fractional limit';
SELECT max(explain LIKE '%Lazily read columns:%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT k, payload
    FROM test_lazy_materialization_with_ties_forms
    ORDER BY tie
    LIMIT 0.5 WITH TIES
);

SELECT 'fractional offset';
SELECT max(explain LIKE '%Lazily read columns:%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT k, payload
    FROM test_lazy_materialization_with_ties_forms
    ORDER BY tie
    LIMIT 3 OFFSET 0.2 WITH TIES
);

SELECT 'negative offset';
SELECT max(explain LIKE '%Lazily read columns:%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT k, payload
    FROM test_lazy_materialization_with_ties_forms
    ORDER BY tie
    LIMIT 3 OFFSET -2 WITH TIES
);

DROP TABLE test_lazy_materialization_with_ties_forms;
