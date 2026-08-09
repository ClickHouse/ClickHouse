-- Pins the documented behavior of INSERT into a *temporary* normal view (issue #91535):
-- `StorageView::write` is implemented on `StorageView` itself, so a temporary view over a
-- non-view table is insertable under exactly the same rules as a persistent one, including
-- the materialization of the target table's DEFAULT for an omitted column.

DROP TABLE IF EXISTS t_insert_into_temporary_view;

CREATE TABLE t_insert_into_temporary_view (a Int32, b Int32 DEFAULT 42) ENGINE = MergeTree ORDER BY a;

CREATE TEMPORARY VIEW v_insert_into_temporary_view AS SELECT a, b FROM t_insert_into_temporary_view;

INSERT INTO v_insert_into_temporary_view (a) VALUES (1);
SELECT 'target:', a, b FROM t_insert_into_temporary_view ORDER BY a;
SELECT 'through-view:', a, b FROM v_insert_into_temporary_view ORDER BY a;

DROP TEMPORARY VIEW v_insert_into_temporary_view;
DROP TABLE t_insert_into_temporary_view;
