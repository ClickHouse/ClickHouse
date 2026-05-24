-- Tags: no-fasttest
-- Verify that optimizeUseRowWrappers produces the same results regardless of
-- whether the rewrite fires (which it does when ≥2 covered columns are
-- requested), and that toggling `query_plan_use_row_wrappers` is safe.

DROP TABLE IF EXISTS row_optimizer;

CREATE TABLE row_optimizer (
    id UInt64,
    a String,
    b UInt32,
    c String,
    combined Row(a String, b UInt32, c String) MATERIALIZED tuple(a, b, c)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO row_optimizer (id, a, b, c) SELECT
    number,
    toString(number),
    toUInt32(number * 2),
    concat('row-', toString(number))
FROM numbers(1000);

-- Same answers regardless of whether the optimizer fires.
SELECT count(), sum(b), max(length(a)) FROM row_optimizer
    SETTINGS query_plan_use_row_wrappers = 1;
SELECT count(), sum(b), max(length(a)) FROM row_optimizer
    SETTINGS query_plan_use_row_wrappers = 0;

-- EXPLAIN with actions enabled should mention the `__rowElement` function
-- in the inserted Expression step when the optimizer rule fires.
SELECT countIf(explain LIKE '%__rowElement%') > 0 FROM (
    EXPLAIN actions = 1 SELECT a, b FROM row_optimizer
    SETTINGS query_plan_use_row_wrappers = 1
);

DROP TABLE row_optimizer;
