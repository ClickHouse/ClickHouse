-- Regression test: when a HAVING expression folds to a constant (e.g., via NULL propagation)
-- and the query uses WITH TOTALS, the totals port header may have different const-ness
-- than the main port header. When combined with UNION DISTINCT, an ExpressionStep applied
-- to both main and totals streams could produce mismatched headers, causing
-- "Block structure mismatch" in Pipe::addSimpleTransform.
-- The fix materializes const-ness differences between main and totals/extremes ports.

DROP TABLE IF EXISTS t_totals_const;
CREATE TABLE t_totals_const (c0 Int32) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_totals_const VALUES (1), (2), (3);

-- Single-branch query: expression folds to const NULL on 0-row headers.
SELECT
    15 AND divide(
        intDivOrZero(toInt256(-9223372036854775807), -2147483649),
        intDivOrZero(MAX(c0) IGNORE NULLS, materialize(NULL))
    ) AND 65536 AS h,
    -c0 AS g
FROM t_totals_const
GROUP BY g
WITH TOTALS
HAVING h;

-- Simpler variant: aggregate expression that folds to const NULL.
SELECT (MAX(c0) + NULL) AS h, -c0 AS g
FROM t_totals_const
GROUP BY g
WITH TOTALS
HAVING h;

-- Original fuzzer query (simplified): UNION DISTINCT with WITH TOTALS on both branches.
-- The UNION step creates converting actions from the main header, but the totals port
-- has different const-ness, causing the mismatch during ExpressionStep::transformPipeline.
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

DROP TABLE t_totals_const;
