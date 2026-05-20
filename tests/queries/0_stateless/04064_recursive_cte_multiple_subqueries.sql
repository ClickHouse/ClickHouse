-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/84318
-- This query previously caused a logical error with recursive CTEs referencing other CTEs.
SET enable_analyzer = 1;
WITH RECURSIVE
    subquery1 AS
    (
        SELECT 1 AS x
        UNION ALL
        SELECT x + 1 AS level
        FROM subquery1
        WHERE x < 5
    ),
    subquery2 AS
    (
        SELECT 1 AS id
        FROM subquery1
    ),
    subquery3 AS
    (
        SELECT id
        FROM subquery2
        UNION ALL
        SELECT cc.id
        FROM subquery3 AS cc
        INNER JOIN subquery2 AS oe ON cc.id = oe.id
        UNION ALL
        SELECT cc.id
        FROM subquery3 AS cc
    )
SELECT *
FROM subquery3
FORMAT Null
SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break', max_result_rows = 1000, result_overflow_mode = 'break';
