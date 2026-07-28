-- Regression test for INSERT into a regular view whose target table has a DEFAULT reading a
-- *subcolumn* of another target column (issue #91535).
--
-- `materializeTargetDefaults` executes the target table's defaults DAG over the forwarded block,
-- which holds whole columns only. A default such as `b DEFAULT obj.x` requires the subcolumn
-- `obj.x`, so the DAG must be preceded by the same subcolumn-extraction step the normal insert path
-- uses. Without it the view path failed with NOT_FOUND_COLUMN_IN_BLOCK while a direct
-- `INSERT INTO t (obj)` derived the default just fine.

DROP TABLE IF EXISTS t_view_subcolumn_default;
DROP VIEW IF EXISTS v_view_subcolumn_default;

CREATE TABLE t_view_subcolumn_default (obj Tuple(x UInt8), b UInt8 DEFAULT obj.x, n Nested(k UInt8), m UInt8 DEFAULT length(n.k))
ENGINE = MergeTree ORDER BY tuple();

CREATE VIEW v_view_subcolumn_default AS SELECT obj, b, n.k, m FROM t_view_subcolumn_default;

-- Direct insert into the target: the default derives from the subcolumn.
INSERT INTO t_view_subcolumn_default (obj, n.k) VALUES ((7), [1, 2]);
SELECT 'direct:', obj.x, b, n.k, m FROM t_view_subcolumn_default ORDER BY b;

-- The same through the view must store the same values.
INSERT INTO v_view_subcolumn_default (obj, `n.k`) VALUES ((9), [1, 2, 3]);
SELECT 'through-view:', obj.x, b, n.k, m FROM t_view_subcolumn_default ORDER BY b;

DROP VIEW v_view_subcolumn_default;
DROP TABLE t_view_subcolumn_default;
