-- Constraint-based constant substitution must not rewrite a correlated column inside a
-- subquery into a constant: correlated columns must stay ColumnNodes for decorrelation.

DROP TABLE IF EXISTS t_const_outer;
DROP TABLE IF EXISTS t_const_inner;

CREATE TABLE t_const_outer (a String, CONSTRAINT c1 ASSUME a = 'x') ENGINE = TinyLog;
INSERT INTO t_const_outer VALUES ('x');

CREATE TABLE t_const_inner (a String) ENGINE = TinyLog;
INSERT INTO t_const_inner VALUES ('x');

SELECT count() FROM t_const_outer WHERE EXISTS (SELECT 1 FROM t_const_inner WHERE t_const_inner.a = t_const_outer.a)
SETTINGS enable_analyzer = 1, optimize_using_constraints = 1, convert_query_to_cnf = 1;

-- The same query with the optimization disabled must return the same value.
SELECT count() FROM t_const_outer WHERE EXISTS (SELECT 1 FROM t_const_inner WHERE t_const_inner.a = t_const_outer.a)
SETTINGS enable_analyzer = 1, optimize_using_constraints = 0, convert_query_to_cnf = 0;

DROP TABLE t_const_inner;
DROP TABLE t_const_outer;
