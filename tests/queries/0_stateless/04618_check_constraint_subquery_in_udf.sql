-- Tags: no-async-insert, no-parallel
-- - no-async-insert -- with wait_for_async_insert=0 the INSERT is fire-and-forget, so the constraint error is raised in the background flush and never reaches the client, breaking the { serverError } assertion.
-- - no-parallel -- SQL UDFs are global server objects; the flaky check runs the same test concurrently and the CREATE FUNCTION statements would collide.

-- A scalar subquery hidden inside a SQL user-defined function must not bypass the
-- CHECK-constraint subquery ban: the Analyzer inlines the UDF body during analysis,
-- so the subquery would otherwise run on every insert.

DROP FUNCTION IF EXISTS f_04618_scalar;
DROP FUNCTION IF EXISTS f_04618_outer;
DROP FUNCTION IF EXISTS f_04618_in_set;
DROP TABLE IF EXISTS check_udf_scalar;
DROP TABLE IF EXISTS check_udf_nested;
DROP TABLE IF EXISTS check_udf_in_set;
DROP TABLE IF EXISTS check_udf_in_set_src;

-- A UDF whose body contains a scalar subquery is rejected.
CREATE FUNCTION f_04618_scalar AS x -> equals((SELECT 1), x);
CREATE TABLE check_udf_scalar (c0 Int, CONSTRAINT c CHECK f_04618_scalar(c0)) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO check_udf_scalar (c0) VALUES (1); -- { serverError BAD_ARGUMENTS }
DROP TABLE check_udf_scalar;

-- The same subquery hidden one UDF level deeper is rejected as well.
CREATE FUNCTION f_04618_outer AS x -> f_04618_scalar(x) OR x > 0;
CREATE TABLE check_udf_nested (c0 Int, CONSTRAINT c CHECK f_04618_outer(c0)) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO check_udf_nested (c0) VALUES (1); -- { serverError BAD_ARGUMENTS }
DROP TABLE check_udf_nested;

-- A UDF that expands to `x IN (subquery)` stays allowed: after expansion it is a direct
-- subquery on the set side of IN, i.e. a "not-ready set" built lazily at insert time.
CREATE TABLE check_udf_in_set_src (id Int) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO check_udf_in_set_src VALUES (1);
CREATE FUNCTION f_04618_in_set AS x -> x IN (SELECT id FROM check_udf_in_set_src);
CREATE TABLE check_udf_in_set (c0 Int, CONSTRAINT c CHECK f_04618_in_set(c0)) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO check_udf_in_set (c0) VALUES (1);
SELECT count() FROM check_udf_in_set;
DROP TABLE check_udf_in_set;
DROP TABLE check_udf_in_set_src;

DROP FUNCTION f_04618_in_set;
DROP FUNCTION f_04618_outer;
DROP FUNCTION f_04618_scalar;
