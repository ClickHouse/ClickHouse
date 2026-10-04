-- A materialized view whose stored SELECT contains a set operation must keep working after its stored
-- definition is re-parsed, both by DETACH/ATTACH and by RESTORE. Parsing fills only the syntactic list of
-- modes, so `ASTSelectWithUnionQuery::union_mode` stays `UNION_DEFAULT`, which the analyzer rejects when
-- pushing an INSERT to the view.
-- Related: https://github.com/ClickHouse/ClickHouse/issues/77569

DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS mv_union;
DROP TABLE IF EXISTS mv_except;
DROP TABLE IF EXISTS mv_except_target;

CREATE TABLE t_src (c0 UInt8) ENGINE = MergeTree ORDER BY c0;

-- UNION ALL in a scalar subquery; materialized view with an inner table.
CREATE MATERIALIZED VIEW mv_union (c0 UInt64) ENGINE = MergeTree ORDER BY c0 AS
    SELECT c0 + (SELECT sum(y) FROM (SELECT 1 AS y UNION ALL SELECT 2 AS y)) AS c0 FROM t_src;

-- EXCEPT ALL in a CTE; materialized view with an explicit target table.
CREATE TABLE mv_except_target (c0 UInt64) ENGINE = MergeTree ORDER BY c0;
CREATE MATERIALIZED VIEW mv_except TO mv_except_target AS
    WITH x AS ((SELECT 1 AS y) EXCEPT ALL (SELECT 2 AS y))
    SELECT c0 + (SELECT sum(y) FROM x) AS c0 FROM t_src;

INSERT INTO t_src VALUES (10);
SELECT 'before attach union', arraySort(groupArray(c0)) FROM mv_union;
SELECT 'before attach except', arraySort(groupArray(c0)) FROM mv_except_target;

DETACH TABLE mv_union SYNC;
ATTACH TABLE mv_union;
DETACH TABLE mv_except SYNC;
ATTACH TABLE mv_except;

INSERT INTO t_src VALUES (20);
SELECT 'after attach union', arraySort(groupArray(c0)) FROM mv_union;
SELECT 'after attach except', arraySort(groupArray(c0)) FROM mv_except_target;

DROP TABLE mv_except;
DROP TABLE mv_except_target;
DROP TABLE mv_union;
DROP TABLE t_src;

-- The same defect reaches a restored view: RESTORE re-parses the stored definition and hands it
-- straight to InterpreterCreateQuery, bypassing executeQuery.
CREATE TABLE t_src_restore (c0 UInt8) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE mv_restore_target (c0 UInt64) ENGINE = MergeTree ORDER BY c0;
CREATE MATERIALIZED VIEW mv_restore TO mv_restore_target AS
    SELECT c0 + (SELECT sum(y) FROM (SELECT 1 AS y UNION DISTINCT SELECT 1 AS y)) AS c0 FROM t_src_restore;
INSERT INTO t_src_restore VALUES (10);
BACKUP TABLE t_src_restore, TABLE mv_restore, TABLE mv_restore_target
    TO Memory('backup_05161_mv_set_operation') FORMAT Null;
DROP TABLE mv_restore SYNC;
DROP TABLE mv_restore_target SYNC;
DROP TABLE t_src_restore SYNC;
RESTORE TABLE t_src_restore, TABLE mv_restore, TABLE mv_restore_target
    FROM Memory('backup_05161_mv_set_operation') FORMAT Null;

INSERT INTO t_src_restore VALUES (20);
SELECT 'after restore', arraySort(groupArray(c0)) FROM mv_restore_target;

DROP TABLE mv_restore;
DROP TABLE mv_restore_target;
DROP TABLE t_src_restore;
