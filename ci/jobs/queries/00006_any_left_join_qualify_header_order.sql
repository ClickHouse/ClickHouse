-- FIXME: Logical error: 'Input header is not a subset of output header'.

-- Test for query optimizer bug with ANY LEFT JOIN + QUALIFY + expressions
-- Bug: addDiscardingExpressionStepIfNeeded in removeUnusedColumns.cpp incorrectly assumed
-- that input and output headers have columns in the same order.
--
-- When processing a query with:
--   1. ANY LEFT JOIN with expression-based join condition (e.g., b + 1 = c + 1)
--   2. Expression in SELECT from the right table (e.g., d + 1)
--   3. QUALIFY clause with an expression (e.g., 2 % 1)
--
-- The query optimizer creates headers with different column orderings, causing the
-- original algorithm to fail with: "Input header is not a subset of output header"
--
-- The fix requires checking column membership by name (using a set) rather than
-- assuming sequential order when comparing input and output headers.

CREATE TABLE t1 (a Int32, b Int32) ENGINE = MergeTree ORDER BY a
CREATE TABLE t2 (c Int32, d Int32) ENGINE = MergeTree ORDER BY c
SELECT d + 1, c FROM t1 ANY LEFT JOIN t2 ON b + 1 = c + 1 QUALIFY 2 % 1
DROP TABLE t1
DROP TABLE t2
