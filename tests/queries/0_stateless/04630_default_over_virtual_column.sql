-- A DEFAULT/MATERIALIZED expression is evaluated over an insert block, which never contains
-- virtual columns, so referencing _table/_database there breaks inserts with
-- NOT_FOUND_COLUMN_IN_BLOCK (a MATERIALIZED column cannot be supplied, so the table is
-- permanently un-insertable). The analyzer must reject such definitions, matching the
-- non-analyzer path. ALIAS (read-time) and EPHEMERAL (a non-stored insert input) may
-- reference virtual columns and keep working. See issue #111658.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_dv;

-- Reject DEFAULT/MATERIALIZED over _table and _database at CREATE TABLE.
CREATE TABLE t_dv (c0 String MATERIALIZED _table, c1 UInt16) ENGINE = MergeTree ORDER BY tuple(); -- { serverError UNKNOWN_IDENTIFIER }
CREATE TABLE t_dv (c0 String DEFAULT _table, c1 UInt16) ENGINE = MergeTree ORDER BY tuple(); -- { serverError UNKNOWN_IDENTIFIER }
CREATE TABLE t_dv (c0 String MATERIALIZED _database, c1 UInt16) ENGINE = MergeTree ORDER BY tuple(); -- { serverError UNKNOWN_IDENTIFIER }
CREATE TABLE t_dv (c0 String DEFAULT _database, c1 UInt16) ENGINE = MergeTree ORDER BY tuple(); -- { serverError UNKNOWN_IDENTIFIER }
-- Reject when the virtual is reached through a function.
CREATE TABLE t_dv (c0 String DEFAULT concat(_table, 'x'), c1 UInt16) ENGINE = MergeTree ORDER BY tuple(); -- { serverError UNKNOWN_IDENTIFIER }
-- Reject when the virtual is reached transitively through an ALIAS/EPHEMERAL column.
CREATE TABLE t_dv (c1 UInt16, ca String ALIAS _table, c0 String MATERIALIZED ca) ENGINE = MergeTree ORDER BY tuple(); -- { serverError UNKNOWN_IDENTIFIER }
CREATE TABLE t_dv (c1 UInt16, ce String EPHEMERAL _table, c0 String DEFAULT ce) ENGINE = MergeTree ORDER BY tuple(); -- { serverError UNKNOWN_IDENTIFIER }

-- Reject on ALTER ADD/MODIFY COLUMN too.
CREATE TABLE t_dv (c1 UInt16, c0 String) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_dv ADD COLUMN c2 String MATERIALIZED _table; -- { serverError UNKNOWN_IDENTIFIER }
ALTER TABLE t_dv MODIFY COLUMN c0 String DEFAULT _database; -- { serverError UNKNOWN_IDENTIFIER }
DROP TABLE t_dv;

-- ALIAS over a virtual column is read-time and must keep working (returns the virtual value).
CREATE TABLE t_dv (c1 UInt16, c0 String ALIAS _table) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dv (c1) VALUES (1);
SELECT c1, c0 FROM t_dv;
DROP TABLE t_dv;

-- EPHEMERAL over a virtual column must keep working (insertable, not stored).
CREATE TABLE t_dv (c1 UInt16, c0 String EPHEMERAL _table) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dv (c1) VALUES (2);
SELECT c1 FROM t_dv;
DROP TABLE t_dv;

-- A read-time ALIAS over a virtual must not poison a sibling insert-time DEFAULT that is clean.
CREATE TABLE t_dv (c1 UInt16, c0 String ALIAS _table, c2 UInt16 DEFAULT c1 + 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dv (c1) VALUES (3);
SELECT c1, c0, c2 FROM t_dv;
DROP TABLE t_dv;

-- Genuine (non-virtual) DEFAULT and MATERIALIZED expressions must still be accepted and usable.
CREATE TABLE t_dv (c1 UInt16, c_def UInt16 DEFAULT c1 + 1, c_mat UInt16 MATERIALIZED c1 * 2) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dv (c1) VALUES (5);
SELECT c1, c_def, c_mat FROM t_dv ORDER BY c1;
DROP TABLE t_dv;

-- A DEFAULT reaching a virtual only through a real column must be accepted (no virtual is used).
CREATE TABLE t_dv (c1 UInt16, ca UInt16 ALIAS c1 + 1, c0 UInt16 DEFAULT ca) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dv (c1) VALUES (5);
SELECT c0 FROM t_dv;
DROP TABLE t_dv;

-- A real column whose name looks like a virtual column must still resolve in a DEFAULT.
CREATE TABLE t_dv (`_table` String, c0 String DEFAULT `_table`, c1 UInt16) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dv (`_table`, c1) VALUES ('real', 7);
SELECT c0, c1 FROM t_dv;
DROP TABLE t_dv;

-- A DEFAULT over a subcolumn of a declared column must still be accepted (not a virtual column).
CREATE TABLE t_dv (a Tuple(x UInt32, y UInt32), b UInt32 DEFAULT a.x) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dv (a) VALUES ((10, 20));
SELECT b FROM t_dv;
DROP TABLE t_dv;

-- An ordinary view's column defaults are never evaluated over an insert block, so a default over a
-- virtual column is inert and must still be accepted.
DROP VIEW IF EXISTS v_dv;
CREATE VIEW v_dv (x String DEFAULT _table) AS SELECT 'ok' AS x;
SELECT x FROM v_dv;
DROP VIEW v_dv;

-- An external-target (TO) materialized view forwards inserts to the target using the target metadata
-- and never evaluates its own column defaults, so a default over a virtual column is inert and must
-- still be accepted (both DEFAULT and MATERIALIZED). The target column has a sentinel default: an
-- insert through the view must fill it from the target default, proving the view default is never
-- evaluated (otherwise the insert would fail on the missing _table column).
DROP TABLE IF EXISTS src_dv;
DROP TABLE IF EXISTS tgt_dv;
DROP TABLE IF EXISTS mv_dv;
CREATE TABLE src_dv (a UInt16) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE tgt_dv (x String DEFAULT 'from_target', a UInt16) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv_dv TO tgt_dv (x String DEFAULT _table, a UInt16) AS SELECT a FROM src_dv;
INSERT INTO src_dv VALUES (1);
SELECT x, a FROM tgt_dv ORDER BY a;
DROP TABLE mv_dv;
CREATE MATERIALIZED VIEW mv_dv TO tgt_dv (x String MATERIALIZED _table, a UInt16) AS SELECT a FROM src_dv;
INSERT INTO src_dv VALUES (2);
SELECT x, a FROM tgt_dv ORDER BY a;
-- A column-modifying ALTER of an external-target view is unsupported by the storage; the virtual-column
-- check must not fire first and mask that with UNKNOWN_IDENTIFIER, so it must still return NOT_IMPLEMENTED.
ALTER TABLE mv_dv MODIFY COLUMN x String DEFAULT _table; -- { serverError NOT_IMPLEMENTED }
DROP TABLE mv_dv;
DROP TABLE src_dv;
DROP TABLE tgt_dv;

-- An inner-table materialized view creates an inner table that DOES apply the view's column defaults on
-- insert, so a DEFAULT/MATERIALIZED over a virtual column is genuinely harmful there and must be rejected.
DROP TABLE IF EXISTS src_dv;
CREATE TABLE src_dv (a UInt16) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv_dv (x String MATERIALIZED _table, a UInt16) ENGINE = MergeTree ORDER BY tuple() AS SELECT a FROM src_dv; -- { serverError UNKNOWN_IDENTIFIER }
DROP TABLE src_dv;

-- A table created by an affected version may already have a default over a virtual column. Loading it
-- (full ATTACH) and an unrelated ALTER on it must still work; the stricter check applies only to new
-- and modified columns, not to the whole existing schema.
set allow_deprecated_database_ordinary = 1;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.legacy (c0 String MATERIALIZED _table, c1 UInt16) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.legacy ADD COLUMN c2 UInt8 DEFAULT 0;
SELECT 'legacy attach and unrelated alter ok';
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
