-- Follow-up review tests for INSERT into regular views (issue #91535).
--
-- 1. Plain `ORDER BY` is ignored on the write path and accepted, but `ORDER BY ... WITH FILL`
--    (and `INTERPOLATE`) synthesize rows at read time, so such views must be rejected at INSERT time.
--
-- 2. A `WHERE` predicate that references a name which is both a view alias and a (different)
--    underlying column (`SELECT t.a AS b, t.b AS a FROM t WHERE a > 0`) is ambiguous: at read
--    time the name resolves to the alias by default but to the underlying column under
--    `prefer_column_name_to_alias = 1`, so there is no single semantics the write-time check
--    could mirror. Such views must be rejected at INSERT time.
--
-- 3. A `WHERE` constraint on a view with non-colliding aliases keeps working: it is evaluated
--    against the value routed into the underlying column the predicate names.

DROP TABLE IF EXISTS t_fill;
DROP VIEW IF EXISTS v_fill;
DROP VIEW IF EXISTS v_interpolate;
DROP TABLE IF EXISTS t_swap;
DROP VIEW IF EXISTS v_swap;
DROP TABLE IF EXISTS t_alias;
DROP VIEW IF EXISTS v_alias;

CREATE TABLE t_fill (a Int32, b String) ENGINE = MergeTree ORDER BY a;

-- 1a. WITH FILL synthesizes rows on read -> reject.
CREATE VIEW v_fill AS SELECT a, b FROM t_fill ORDER BY a WITH FILL FROM 0 TO 10;
INSERT INTO v_fill VALUES (1, 'x'); -- { serverError NOT_IMPLEMENTED }

-- 1b. INTERPOLATE is part of the WITH FILL machinery -> reject.
CREATE VIEW v_interpolate AS SELECT a, b FROM t_fill ORDER BY a WITH FILL INTERPOLATE (b AS b);
INSERT INTO v_interpolate VALUES (2, 'y'); -- { serverError NOT_IMPLEMENTED }

-- 2. Alias swap with a WHERE that references a colliding name -> ambiguous -> reject.
--    Columns in the SELECT list are qualified with the table name so the alias swap is not
--    misread as a cyclic alias by the old analyzer (`SELECT a AS b, b AS a` is `CYCLIC_ALIASES`
--    there).
CREATE TABLE t_swap (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
CREATE VIEW v_swap AS SELECT t_swap.a AS b, t_swap.b AS a FROM t_swap WHERE a > 0;
INSERT INTO v_swap (b, a) VALUES (-1, 100); -- { serverError NOT_IMPLEMENTED }
INSERT INTO v_swap (b, a) VALUES (7, 100); -- { serverError NOT_IMPLEMENTED }
SELECT 'swap:', count() FROM t_swap;

-- 3. Non-colliding alias with a WHERE constraint. The view exposes `id` for the underlying `a`;
--    the WHERE constrains the underlying `a`, which receives the value inserted as `id`.
CREATE TABLE t_alias (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
CREATE VIEW v_alias AS SELECT a AS id, b FROM t_alias WHERE a > 0;
INSERT INTO v_alias (id, b) VALUES (-1, 100); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO v_alias (id, b) VALUES (7, 100);
SELECT 'alias:', a, b FROM t_alias ORDER BY a;

DROP VIEW v_alias;
DROP TABLE t_alias;
DROP VIEW v_swap;
DROP TABLE t_swap;
DROP VIEW v_interpolate;
DROP VIEW v_fill;
DROP TABLE t_fill;
