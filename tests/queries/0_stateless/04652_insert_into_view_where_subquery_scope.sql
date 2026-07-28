-- Regression test for INSERT into a regular view whose WHERE contains an independent subquery while
-- the SELECT list renames colliding column names (issue #91535).
--
-- A SELECT-list alias may collide with the name of a *different* underlying column (an alias swap
-- such as `SELECT t.a AS b, t.b AS a FROM t`), and an unqualified reference to such a name in the
-- WHERE is ambiguous, so the view is rejected as not insertable. Identifiers inside a subquery in
-- the WHERE belong to the subquery's own tables, not to the view's input, so they must not arm that
-- guard. Before the fix, the guard walked into subquery scopes, so a view such as
-- `... WHERE 1 IN (SELECT a FROM allowed)` was rejected with NOT_IMPLEMENTED even though its outer
-- predicate never references the colliding name.

DROP TABLE IF EXISTS t_view_where_subquery;
DROP TABLE IF EXISTS allowed_view_where_subquery;
DROP VIEW IF EXISTS v_view_where_subquery;
DROP VIEW IF EXISTS v_view_where_subquery_outer_ref;

CREATE TABLE t_view_where_subquery (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE allowed_view_where_subquery (a Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO allowed_view_where_subquery VALUES (1);

-- The colliding name `a` appears only inside an independent subquery -> the outer predicate is
-- unambiguous -> the view is insertable, and the subquery is enforced as a constraint.
CREATE VIEW v_view_where_subquery AS
    SELECT t_view_where_subquery.a AS b, t_view_where_subquery.b AS a
    FROM t_view_where_subquery
    WHERE 1 IN (SELECT a FROM allowed_view_where_subquery);

INSERT INTO v_view_where_subquery (b, a) VALUES (10, 20);
SELECT 'subquery-ok:', a, b FROM t_view_where_subquery ORDER BY a;
SELECT 'read-back:', b, a FROM v_view_where_subquery ORDER BY b;

-- The subquery constraint is still evaluated: with no matching row the condition is false and the
-- insert is rejected.
TRUNCATE TABLE allowed_view_where_subquery;
INSERT INTO v_view_where_subquery (b, a) VALUES (11, 21); -- { serverError VIOLATED_CONSTRAINT }

-- A reference to the colliding name in the *outer* predicate is still ambiguous and still rejected,
-- even when a subquery is present as well.
CREATE VIEW v_view_where_subquery_outer_ref AS
    SELECT t_view_where_subquery.a AS b, t_view_where_subquery.b AS a
    FROM t_view_where_subquery
    WHERE a > 0 AND 1 IN (SELECT a FROM allowed_view_where_subquery);

INSERT INTO v_view_where_subquery_outer_ref (b, a) VALUES (10, 20); -- { serverError NOT_IMPLEMENTED }

DROP VIEW v_view_where_subquery_outer_ref;
DROP VIEW v_view_where_subquery;
DROP TABLE allowed_view_where_subquery;
DROP TABLE t_view_where_subquery;
