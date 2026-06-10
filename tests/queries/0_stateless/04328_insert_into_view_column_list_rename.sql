-- INSERT into a view that renames columns via the explicit view column list
-- (`CREATE VIEW v (id, name) AS SELECT a, b FROM t`) rather than via SELECT-list aliases.
-- The column list is rewritten into SELECT-list aliases (`SELECT a AS id, b AS name`) when
-- the view is created, so the insert must route `id -> a` and `name -> b` exactly like an
-- alias-renamed view.

DROP TABLE IF EXISTS t_cl;
DROP VIEW IF EXISTS v_cl;
DROP TABLE IF EXISTS t_cl_where;
DROP VIEW IF EXISTS v_cl_where;

CREATE TABLE t_cl (a Int32, b String) ENGINE = MergeTree ORDER BY a;
CREATE VIEW v_cl (id, name) AS SELECT a, b FROM t_cl;
INSERT INTO v_cl VALUES (1, 'x');
INSERT INTO v_cl (id, name) VALUES (2, 'y');
SELECT 'column-list:', a, b FROM t_cl ORDER BY a;

-- A column-list rename combined with a WHERE constraint: the renames do not collide with
-- underlying column names, so the constraint stays unambiguous and is enforced against the
-- value routed into the underlying `a` (inserted as `id`).
CREATE TABLE t_cl_where (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
CREATE VIEW v_cl_where (id, val) AS SELECT a, b FROM t_cl_where WHERE a > 0;
INSERT INTO v_cl_where (id, val) VALUES (-1, 100); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO v_cl_where (id, val) VALUES (7, 100);
SELECT 'column-list-where:', a, b FROM t_cl_where ORDER BY a;

DROP VIEW v_cl_where;
DROP TABLE t_cl_where;
DROP VIEW v_cl;
DROP TABLE t_cl;
