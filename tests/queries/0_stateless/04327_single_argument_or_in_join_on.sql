DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (c0 Int) ENGINE = Memory;

-- An empty ARRAY JOIN makes `a0` Nothing-typed, so `a0 = t1.c0` is Nothing-typed and
-- `or(...)` of it resolves to a single-argument `or` (the Nothing short-circuit skips the
-- arity check). The common-expression extraction pass used to assert >= 2 arguments and abort.
SELECT t1.c0 FROM t1 AS tx LEFT ARRAY JOIN [] AS a0 LEFT JOIN t1 ON or(a0 = t1.c0);
SELECT t1.c0 FROM t1 AS tx LEFT ARRAY JOIN [] AS a0 LEFT JOIN t1 ON or(or(a0 = t1.c0));
SELECT t1.c0 FROM t1 AS tx LEFT ARRAY JOIN [] AS a0 LEFT JOIN t1 ON or(and(a0 = t1.c0, a0 = tx.c0)); -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- A single-argument `or` outside JOIN ON must still be rejected with a clean error.
SELECT t1.c0 FROM t1 AS tx LEFT ARRAY JOIN [] AS a0 WHERE or(a0 = t1.c0); -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

DROP TABLE t1;
