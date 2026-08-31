-- Tags: no-old-analyzer
-- The fix is in the analyzer's `LogicalExpressionOptimizerPass`; the old analyzer rejects these
-- `JOIN ON` shapes via a different path (`INVALID_JOIN_ON_EXPRESSION`), so its output differs.

-- The `JOIN ON` extraction runs only under this setting, which the test runner randomizes.
SET optimize_extract_common_expressions = 1;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (c0 Int) ENGINE = Memory;

-- An empty `ARRAY JOIN` makes `a0` `Nothing`-typed, so `a0 = t1.c0` is `Nothing`-typed and
-- `or(...)` of it resolves to a single-argument `or` (the `Nothing` short-circuit skips the
-- arity check). The common-expression extraction pass used to assert >= 2 arguments and abort.
SELECT t1.c0 FROM t1 AS tx LEFT ARRAY JOIN [] AS a0 LEFT JOIN t1 ON or(a0 = t1.c0);
-- A nested single-argument `or` reaches the pass the same way.
SELECT t1.c0 FROM t1 AS tx LEFT ARRAY JOIN [] AS a0 LEFT JOIN t1 ON or(or(a0 = t1.c0));
-- A single-argument `or` wrapping a `Nothing`-typed `and` also falls through the pass gracefully;
-- the analyzer accepts the `Nothing`-typed `ON` and the query returns an empty result (t1 is empty).
SELECT t1.c0 FROM t1 AS tx LEFT ARRAY JOIN [] AS a0 LEFT JOIN t1 ON or(and(a0 = t1.c0, a0 = tx.c0));

-- A single-argument `or` outside `JOIN ON` must still be rejected with a clean error.
SELECT t1.c0 FROM t1 AS tx LEFT ARRAY JOIN [] AS a0 WHERE or(a0 = t1.c0); -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

-- `convert_query_to_cnf=1` routes `WHERE`/`PREWHERE`/`HAVING` through `ConvertLogicalExpressionToCNFPass`
-- (whose CNF builder assumes binary `or`). A single-argument `or` never reaches it: it is always
-- `Nothing`-typed (the `Nothing` short-circuit is the only way arity validation is skipped), so the
-- filter is rejected as a non-boolean type during query analysis, before the CNF pass runs.
SELECT t1.c0 FROM t1 AS tx LEFT ARRAY JOIN [] AS a0 WHERE or(a0 = t1.c0) SETTINGS convert_query_to_cnf = 1; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }
SELECT t1.c0 FROM t1 AS tx LEFT ARRAY JOIN [] AS a0 PREWHERE or(a0 = t1.c0) SETTINGS convert_query_to_cnf = 1; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }
SELECT t1.c0 FROM t1 AS tx LEFT ARRAY JOIN [] AS a0 GROUP BY t1.c0 HAVING or(a0 = t1.c0) SETTINGS convert_query_to_cnf = 1; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

DROP TABLE t1;
