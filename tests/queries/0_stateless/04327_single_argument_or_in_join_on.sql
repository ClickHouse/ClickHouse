-- Tags: no-old-analyzer
-- The fix is in the new analyzer's LogicalExpressionOptimizerPass; the old analyzer rejects
-- these JOIN ON shapes via a different path (INVALID_JOIN_ON_EXPRESSION), so its output differs.

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (c0 Int) ENGINE = Memory;

-- An empty ARRAY JOIN makes `a0` Nothing-typed, so `a0 = t1.c0` is Nothing-typed and
-- `or(...)` of it resolves to a single-argument `or` (the Nothing short-circuit skips the
-- arity check). The common-expression extraction pass used to assert >= 2 arguments and abort.
SELECT t1.c0 FROM t1 AS tx LEFT ARRAY JOIN [] AS a0 LEFT JOIN t1 ON or(a0 = t1.c0);
-- optimize_or_like_chain=0: with the rewrite on, the nested or is folded to a constant and the
-- JOIN ON is rejected (INVALID_JOIN_ON_EXPRESSION) before reaching the pass this test exercises.
SELECT t1.c0 FROM t1 AS tx LEFT ARRAY JOIN [] AS a0 LEFT JOIN t1 ON or(or(a0 = t1.c0)) SETTINGS optimize_or_like_chain = 0;
SELECT t1.c0 FROM t1 AS tx LEFT ARRAY JOIN [] AS a0 LEFT JOIN t1 ON or(and(a0 = t1.c0, a0 = tx.c0)); -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- A single-argument `or` outside JOIN ON must still be rejected with a clean error.
SELECT t1.c0 FROM t1 AS tx LEFT ARRAY JOIN [] AS a0 WHERE or(a0 = t1.c0); -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

DROP TABLE t1;
