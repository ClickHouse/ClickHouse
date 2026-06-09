-- Follow-up review tests for INSERT into regular views (issue #91535).
--
-- 1. Plain `ORDER BY` is ignored on the write path and accepted, but `ORDER BY ... WITH FILL`
--    (and `INTERPOLATE`) synthesize rows at read time, so such views must be rejected at INSERT time.
--
-- 2. A `WHERE` predicate on a view whose output aliases swap the underlying columns
--    (`SELECT a AS b, b AS a FROM t WHERE a > 0`) must be checked against the value that is
--    actually stored in the predicate's column, not against a colliding view-column name.

DROP TABLE IF EXISTS t_fill;
DROP VIEW IF EXISTS v_fill;
DROP VIEW IF EXISTS v_interpolate;
DROP TABLE IF EXISTS t_swap;
DROP VIEW IF EXISTS v_swap;

CREATE TABLE t_fill (a Int32, b String) ENGINE = MergeTree ORDER BY a;

-- 1a. WITH FILL synthesizes rows on read -> reject.
CREATE VIEW v_fill AS SELECT a, b FROM t_fill ORDER BY a WITH FILL FROM 0 TO 10;
INSERT INTO v_fill VALUES (1, 'x'); -- { serverError NOT_IMPLEMENTED }

-- 1b. INTERPOLATE is part of the WITH FILL machinery -> reject.
CREATE VIEW v_interpolate AS SELECT a, b FROM t_fill ORDER BY a WITH FILL INTERPOLATE (b AS b);
INSERT INTO v_interpolate VALUES (2, 'y'); -- { serverError NOT_IMPLEMENTED }

-- 2. Alias swap with a WHERE constraint. The view exposes `b` for the underlying `a` and `a`
--    for the underlying `b`; the WHERE constrains the underlying `a`. The constraint must be
--    evaluated against the underlying `a` value, not the same-named view column.
CREATE TABLE t_swap (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
CREATE VIEW v_swap AS SELECT a AS b, b AS a FROM t_swap WHERE a > 0;

-- Underlying a = -1 (view column `b`), so the constraint `a > 0` is violated and the row must be rejected.
INSERT INTO v_swap (b, a) VALUES (-1, 100); -- { serverError VIOLATED_CONSTRAINT }

-- Underlying a = 7 (view column `b`), so the constraint is satisfied and the row is stored as (a, b) = (7, 100).
INSERT INTO v_swap (b, a) VALUES (7, 100);
SELECT 'swap:', a, b FROM t_swap ORDER BY a;

DROP VIEW v_swap;
DROP TABLE t_swap;
DROP VIEW v_interpolate;
DROP VIEW v_fill;
DROP TABLE t_fill;
