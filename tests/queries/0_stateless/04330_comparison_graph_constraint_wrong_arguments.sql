DROP TABLE IF EXISTS t_cg_args;

-- ASSUME constraints skip argument-count validation, so a comparison with the
-- wrong number of arguments reaches ComparisonGraph::normalizeAtom and must be
-- skipped instead of crashing the server (see ComparisonGraph.cpp).
CREATE TABLE t_cg_args (a Int32, b Int32, CONSTRAINT c ASSUME less(a)) ENGINE = MergeTree ORDER BY a;
DROP TABLE t_cg_args;

CREATE TABLE t_cg_args (a Int32, b Int32, CONSTRAINT c ASSUME lessOrEquals()) ENGINE = MergeTree ORDER BY a;
DROP TABLE t_cg_args;

CREATE TABLE t_cg_args (a Int32, b Int32, CONSTRAINT c ASSUME less(a, a, a)) ENGINE = MergeTree ORDER BY a;
DROP TABLE t_cg_args;

CREATE TABLE t_cg_args (a Int32, b Int32, CONSTRAINT c ASSUME greaterOrEquals(b)) ENGINE = MergeTree ORDER BY a;
DROP TABLE t_cg_args;

SELECT 'ok';
