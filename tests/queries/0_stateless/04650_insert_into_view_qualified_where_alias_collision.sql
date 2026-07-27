-- Regression test for INSERT into a regular view whose WHERE qualifies a colliding column name
-- with the underlying table name or its alias (issue #91535).
--
-- A SELECT-list alias may collide with the name of a *different* underlying column (an alias swap
-- such as `SELECT t.a AS b, t.b AS a FROM t`). An unqualified `WHERE a > 0` is then ambiguous — the
-- name resolves to the alias by default but to the underlying column under
-- `prefer_column_name_to_alias = 1` — so such a view is rejected as not insertable. A *qualified*
-- reference `t.a` is not ambiguous: an alias is always a single-part name, so a qualified identifier
-- can only mean the underlying column. Before the fix, the guard recorded the short name of
-- qualified identifiers too, and the constraint analysis could not resolve them, so these views were
-- rejected with NOT_IMPLEMENTED as well.

DROP TABLE IF EXISTS t_view_qualified_where;
DROP VIEW IF EXISTS v_view_qualified_where;
DROP VIEW IF EXISTS v_view_qualified_where_alias;
DROP VIEW IF EXISTS v_view_unqualified_where;

CREATE TABLE t_view_qualified_where (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;

-- The WHERE qualifies the colliding name with the table name -> unambiguous -> insertable.
-- The view column `b` maps to the underlying `a`, so `t.a > 0` constrains the first inserted value.
CREATE VIEW v_view_qualified_where AS
    SELECT t_view_qualified_where.a AS b, t_view_qualified_where.b AS a
    FROM t_view_qualified_where
    WHERE t_view_qualified_where.a > 0;

INSERT INTO v_view_qualified_where (b, a) VALUES (10, 20);
SELECT 'qualified-ok:', a, b FROM t_view_qualified_where ORDER BY a;
SELECT 'read-back:', b, a FROM v_view_qualified_where ORDER BY b;

-- The constraint is still enforced, and against the underlying column the qualified name denotes.
INSERT INTO v_view_qualified_where (b, a) VALUES (-1, 5); -- { serverError VIOLATED_CONSTRAINT }

-- The same, with the qualifier being the table alias instead of the table name.
CREATE VIEW v_view_qualified_where_alias AS
    SELECT tt.a AS b, tt.b AS a
    FROM t_view_qualified_where AS tt
    WHERE tt.a > 0;

INSERT INTO v_view_qualified_where_alias (b, a) VALUES (30, 40);
SELECT 'alias-qualified-ok:', a, b FROM t_view_qualified_where ORDER BY a;

-- An unqualified reference to the colliding name remains ambiguous and is still rejected.
CREATE VIEW v_view_unqualified_where AS
    SELECT t_view_qualified_where.a AS b, t_view_qualified_where.b AS a
    FROM t_view_qualified_where
    WHERE a > 0;

INSERT INTO v_view_unqualified_where (b, a) VALUES (10, 20); -- { serverError NOT_IMPLEMENTED }

DROP VIEW v_view_unqualified_where;
DROP VIEW v_view_qualified_where_alias;
DROP VIEW v_view_qualified_where;
DROP TABLE t_view_qualified_where;
