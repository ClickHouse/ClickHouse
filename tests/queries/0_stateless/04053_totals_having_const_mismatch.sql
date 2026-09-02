-- Regression test for block structure mismatch between main and totals streams.
--
-- Root cause: `defaultImplementationForNulls` in IFunction.cpp returned a non-const
-- column at 0 rows even when all arguments were constant. This caused
-- `ActionsDAG::updateHeader` and `ExpressionActions::execute` to produce different
-- column constness for expressions involving `concat` (which uses default null handling)
-- vs `divide`/`intDivOrZero` (which handle nulls themselves).
--
-- When two UNION DISTINCT branches had different constness for the same output column,
-- the Union step's converting actions would fail with "Block structure mismatch" or
-- "non constant in source stream but must be constant in result".

DROP TABLE IF EXISTS t_totals_const;
CREATE TABLE t_totals_const (c0 Int32) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_totals_const VALUES (1), (2), (3);

-- Original fuzzer query: two branches with WITH TOTALS, the first uses `divide`
-- (custom null handling → const at 0 rows) and the second uses `concat`
-- (default null handling → was non-const at 0 rows).
SELECT
    15 AND divide(
        intDivOrZero(toInt256(-9223372036854775807), -2147483649),
        intDivOrZero(MAX(c0) IGNORE NULLS, materialize(NULL))
    ) AND 65536 AS h,
    -c0 AS g
FROM t_totals_const
GROUP BY g
WITH TOTALS
HAVING h
LIMIT 1048575
UNION DISTINCT
SELECT
    7 AND concat(
        intDivOrZero((SELECT -9223372036854775807, toNullable(NULL)), -2),
        intDivOrZero(materialize(NULL), MAX(c0) IGNORE NULLS)
    ) AND 65535 AS h,
    -c0 AS g
FROM t_totals_const
GROUP BY g
WITH TOTALS
HAVING h;

-- Simpler variant: single branch with expression that folds to const NULL.
SELECT (MAX(c0) + NULL) AS h, -c0 AS g
FROM t_totals_const
GROUP BY g
WITH TOTALS
HAVING h;

DROP TABLE t_totals_const;
