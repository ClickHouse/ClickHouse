-- Regression test for INSERT into a regular view whose WHERE uses a name only as a lambda
-- parameter (issue #91535).
--
-- A SELECT-list alias may collide with the name of a *different* underlying column (an alias swap
-- such as `SELECT t.a AS b, t.b AS a FROM t`). If the WHERE references such a name as a column, the
-- read-time semantics are ambiguous and the view is rejected at INSERT time. But a lambda parameter
-- of the same name (e.g. `a` in `arrayExists(a -> a > 0, [1])`) shadows the outer column within the
-- lambda body, so it is a local binding rather than a reference to the colliding name and must not
-- trigger the guard. Before the scope-aware fix, the guard recorded lambda parameters too and
-- wrongly rejected such views with NOT_IMPLEMENTED. Columns are qualified with the table name so the
-- swap is not misread as a cyclic alias by the old analyzer.

DROP TABLE IF EXISTS t_view_lambda;
DROP VIEW IF EXISTS v_view_lambda;
DROP VIEW IF EXISTS v_view_lambda_ref;

CREATE TABLE t_view_lambda (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;

-- The WHERE uses `a` only as a lambda parameter -> unambiguous -> insertable.
CREATE VIEW v_view_lambda AS SELECT t_view_lambda.a AS b, t_view_lambda.b AS a FROM t_view_lambda WHERE arrayExists(a -> a > 0, [1]);
INSERT INTO v_view_lambda (b, a) VALUES (10, 20);
SELECT 'lambda-ok:', a, b FROM t_view_lambda ORDER BY a;

-- The same lambda parameter combined with a genuine outer reference to the colliding name is still
-- ambiguous -> rejected.
CREATE VIEW v_view_lambda_ref AS SELECT t_view_lambda.a AS b, t_view_lambda.b AS a FROM t_view_lambda WHERE a > 0 AND arrayExists(a -> a > 0, [1]);
INSERT INTO v_view_lambda_ref (b, a) VALUES (10, 20); -- { serverError NOT_IMPLEMENTED }

DROP VIEW v_view_lambda_ref;
DROP VIEW v_view_lambda;
DROP TABLE t_view_lambda;
