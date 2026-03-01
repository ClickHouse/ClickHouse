-- Regression test for block structure mismatch in removeUnusedColumns optimization
-- when JoinStepLogical materializes its __join_result_dummy column but the parent
-- step's input header retains a stale column representation (Const vs materialized).
-- See https://github.com/ClickHouse/ClickHouse/issues/92845

-- Queries that exercise the removeUnusedColumns + JoinStepLogical interaction
-- with various plan structures. These must not cause LOGICAL_ERROR exceptions
-- in debug builds.

-- Basic: join where no output columns are used (triggers __join_result_dummy)
SELECT count() FROM (SELECT 1 AS a) AS t1 INNER JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a;
SELECT 1 FROM (SELECT 1 AS a) AS t1 INNER JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a;

-- With subquery wrapping (adds ExpressionSteps that may be merged)
SELECT count() FROM (SELECT * FROM (SELECT 1 AS a) AS t1 INNER JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a);
SELECT count() FROM (SELECT 1 AS x FROM (SELECT 1 AS a) AS t1 INNER JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a);

-- With GROUP BY modifiers (ROLLUP, CUBE, TOTALS) that affect plan structure
SELECT count() FROM (SELECT DISTINCT CAST('1', 'Int32') AS a GROUP BY 1 WITH ROLLUP) AS t1 INNER JOIN (SELECT DISTINCT CAST('2', 'UInt32') AS a GROUP BY 1 WITH TOTALS) AS t2 ON t1.a = t2.a;

-- Reproducer pattern from the original issue (#92845): uses *, ROLLUP, TOTALS, and QUALIFY
SELECT count() FROM (
    SELECT *, and(*, 1, 0)
    FROM (SELECT DISTINCT CAST('1', 'Int32') AS a GROUP BY 1 WITH ROLLUP) AS t1
    INNER JOIN (SELECT DISTINCT CAST('2', 'UInt32') AS a GROUP BY 1 WITH TOTALS) AS t2
    ON t1.a = t2.a
);

-- Various join types where output columns are unused
SELECT count() FROM (SELECT 1 AS a) AS t1 LEFT JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a;
SELECT count() FROM (SELECT 1 AS a) AS t1 RIGHT JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a;
SELECT count() FROM (SELECT 1 AS a) AS t1 FULL JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a;
SELECT count() FROM (SELECT 1 AS a) AS t1 CROSS JOIN (SELECT 1 AS a) AS t2;
SELECT count() FROM (SELECT 1 AS a) AS t1 ANTI JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a;
SELECT count() FROM (SELECT 1 AS a) AS t1 SEMI JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a;

-- Nested joins where intermediate results are unused
SELECT count() FROM (
    SELECT 1 FROM (SELECT 1 AS a) AS t1 INNER JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a
) sub1
INNER JOIN (SELECT 1 AS a) AS t3 ON 1 = 1;

-- With join_use_nulls (toNullable wrapping adds FUNCTION nodes subject to constant folding)
SELECT count() FROM (SELECT 1 AS a) AS t1 LEFT JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a SETTINGS join_use_nulls = 1;
SELECT count() FROM (SELECT 1 AS a) AS t1 RIGHT JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a SETTINGS join_use_nulls = 1;
SELECT count() FROM (SELECT 1 AS a) AS t1 FULL JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a SETTINGS join_use_nulls = 1;
