-- Regression test for a logical error in the old analyzer when a null-safe join
-- key (operator <=> / IS NOT DISTINCT FROM) is a non-column expression that is not
-- materialized into the join input block, e.g. a scalar subquery. Previously this
-- aborted with LOGICAL_ERROR "Can't find key _subqueryN in left columns [...]".
-- Now it behaves like the non null-safe path: analysis succeeds and the query fails
-- (at execution) with a proper NOT_FOUND_COLUMN_IN_BLOCK error instead of crashing.

DROP TABLE IF EXISTS t_nsjk_1;
DROP TABLE IF EXISTS t_nsjk_2;

CREATE TABLE t_nsjk_1 (lk Nullable(Int32), a Int32) ENGINE = Memory;
CREATE TABLE t_nsjk_2 (rk Nullable(Int32)) ENGINE = Memory;

SET enable_analyzer = 0;
SET explain_query_plan_default = 'legacy';

-- The exact shape found by the AST fuzzer (EXPLAIN SYNTAX) must not crash.
-- Wrapped in count() so the volatile auto-generated subquery alias is not printed.
SELECT count() > 0 FROM (EXPLAIN SYNTAX SELECT a FROM t_nsjk_1 ALL INNER JOIN t_nsjk_2 ON (SELECT lk FROM t_nsjk_1) <=> t_nsjk_2.rk);

-- Executing such a query must produce a regular error, not abort the server.
SELECT a FROM t_nsjk_1 ALL INNER JOIN t_nsjk_2 ON (SELECT lk FROM t_nsjk_1) <=> t_nsjk_2.rk; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

-- A regular null-safe join over plain columns still works and matches NULL <=> NULL.
INSERT INTO t_nsjk_1 VALUES (5, 100), (NULL, 200);
INSERT INTO t_nsjk_2 VALUES (5), (NULL);
SELECT a FROM t_nsjk_1 ALL INNER JOIN t_nsjk_2 ON t_nsjk_1.lk <=> t_nsjk_2.rk ORDER BY a;

DROP TABLE t_nsjk_1;
DROP TABLE t_nsjk_2;
