-- Tags: no-random-settings

DROP TABLE IF EXISTS test_lazy_materialization_with_ties_setting;

CREATE TABLE test_lazy_materialization_with_ties_setting
(
    k UInt64,
    tie UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO test_lazy_materialization_with_ties_setting
SELECT
    number,
    intDiv(number, 5),
    repeat('payload', 16)
FROM numbers(20);

SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 10;

SELECT 'default disabled';
SELECT countIf(explain LIKE '%Lazily read columns:%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT k, payload
    FROM test_lazy_materialization_with_ties_setting
    ORDER BY tie
    LIMIT 3 WITH TIES
);

SELECT 'explicitly enabled';
SELECT countIf(explain LIKE '%Lazily read columns:%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT k, payload
    FROM test_lazy_materialization_with_ties_setting
    ORDER BY tie
    LIMIT 3 WITH TIES
    SETTINGS query_plan_optimize_lazy_materialization_with_ties = 1
);

SELECT 'bounded by max limit';
SELECT countIf(explain LIKE '%Lazily read columns:%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT k, payload
    FROM test_lazy_materialization_with_ties_setting
    ORDER BY tie
    LIMIT 11 WITH TIES
    SETTINGS query_plan_optimize_lazy_materialization_with_ties = 1
);

SELECT 'unbounded max enables with ties';
SELECT countIf(explain LIKE '%Lazily read columns:%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT k, payload
    FROM test_lazy_materialization_with_ties_setting
    ORDER BY tie
    LIMIT 11 WITH TIES
    SETTINGS query_plan_max_limit_for_lazy_materialization = 0
);

SELECT 'result check';
SELECT count(), sum(k)
FROM
(
    SELECT k, payload
    FROM test_lazy_materialization_with_ties_setting
    ORDER BY tie
    LIMIT 3 WITH TIES
    SETTINGS query_plan_optimize_lazy_materialization_with_ties = 1
);

DROP TABLE test_lazy_materialization_with_ties_setting;
