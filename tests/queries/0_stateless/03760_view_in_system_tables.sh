#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Secondary databases. Suffixed with $CLICKHOUSE_DATABASE so parallel runs of
# this test don't collide on shared global names.
DB_X="${CLICKHOUSE_DATABASE}_x"
DB_RENAME_BEFORE="${CLICKHOUSE_DATABASE}_rb"
DB_RENAME_AFTER="${CLICKHOUSE_DATABASE}_ra"
DB_RENAME_EXT="${CLICKHOUSE_DATABASE}_rext"

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB_X}"
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB_RENAME_BEFORE}"
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB_RENAME_AFTER}"
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB_RENAME_EXT}"

$CLICKHOUSE_CLIENT <<EOF
DROP TABLE IF EXISTS 03760_src1;
DROP TABLE IF EXISTS 03760_src2;
DROP VIEW IF EXISTS 03760_view1;
DROP VIEW IF EXISTS 03760_view2;
DROP VIEW IF EXISTS 03760_view3;
DROP VIEW IF EXISTS 03760_mview1;

CREATE TABLE 03760_src1 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE 03760_src2 (id UInt64, data String) ENGINE = MergeTree ORDER BY id;

CREATE VIEW 03760_view1 AS SELECT * FROM 03760_src1;
CREATE VIEW 03760_view2 AS SELECT 03760_src1.id, 03760_src1.value, 03760_src2.data FROM 03760_src1 JOIN 03760_src2 ON 03760_src1.id = 03760_src2.id;
CREATE VIEW 03760_view3 AS SELECT * FROM 03760_view1;

CREATE MATERIALIZED VIEW 03760_mview1 ENGINE = MergeTree ORDER BY id AS SELECT * FROM 03760_src1;

-- 03760_src1 should show 03760_view1, 03760_view2, and 03760_mview1 as dependents (direct only; 03760_view3 depends on 03760_view1, not src1)
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src1';

-- 03760_src2 should show 03760_view2 as dependent
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src2';

-- 03760_view1 should show 03760_view3 as dependent
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_view1';

-- 03760_view2 and 03760_view3 themselves must not have dependents
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_view2';
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_view3';

-- Check all tables and their dependencies (excluding internal MV storage tables)
SELECT name, engine, arraySort(dependencies_table) as deps
FROM system.tables
WHERE database = currentDatabase() AND NOT name LIKE '.inner%'
ORDER BY name;

-- CREATE OR REPLACE VIEW: dependencies must reflect the new query (old source loses view, new source gains it)
DROP VIEW IF EXISTS 03760_repl_view;
CREATE VIEW 03760_repl_view AS SELECT * FROM 03760_src1;
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src1';
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src2';

CREATE OR REPLACE VIEW 03760_repl_view AS SELECT id, data FROM 03760_src2;
-- After replace: 03760_src1 should no longer list 03760_repl_view; 03760_src2 should list it
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src1';
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src2';

DROP VIEW 03760_repl_view;

-- ALTER VIEW ... MODIFY QUERY (Materialized View): dependencies must reflect the new query
DROP TABLE IF EXISTS 03760_mv_dest;
DROP VIEW IF EXISTS 03760_mv_alter;
CREATE TABLE 03760_mv_dest (id UInt64, data String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW 03760_mv_alter TO 03760_mv_dest AS SELECT id, value AS data FROM 03760_src1;
-- 03760_src1 should list 03760_mv_alter
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src1';

SET allow_experimental_alter_materialized_view_structure = 1;
ALTER TABLE 03760_mv_alter MODIFY QUERY SELECT id, data FROM 03760_src2;
-- After alter: 03760_src1 should no longer list 03760_mv_alter; 03760_src2 should list it
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src1';
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src2';

DROP VIEW 03760_mv_alter;
DROP TABLE 03760_mv_dest;
EOF

# Cross-database section
$CLICKHOUSE_CLIENT <<EOF
CREATE DATABASE ${DB_X};
CREATE TABLE ${DB_X}.remote_t (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW 03760_local_view_of_x AS SELECT * FROM ${DB_X}.remote_t;
CREATE VIEW ${DB_X}.remote_view_of_local AS SELECT * FROM 03760_src1;

-- 03760_src1 should list ${DB_X}.remote_view_of_local among dependents (view in other db depends on current db table)
SELECT concat(dependencies_database, '.', dependencies_table) AS dep
FROM system.tables
ARRAY JOIN dependencies_database, dependencies_table
WHERE database = currentDatabase() AND name = '03760_src1'
ORDER BY dep;

-- ${DB_X}.remote_t should list 03760_local_view_of_x as dependent (view in current db depends on other db table)
SELECT DISTINCT dependencies_table AS dep
FROM system.tables
ARRAY JOIN dependencies_table
WHERE database = '${DB_X}' AND name = 'remote_t'
ORDER BY dep;

DROP VIEW ${DB_X}.remote_view_of_local;
DROP VIEW 03760_local_view_of_x;
DROP TABLE ${DB_X}.remote_t;
DROP DATABASE ${DB_X};

DROP VIEW 03760_view3;
DROP VIEW 03760_view2;
DROP VIEW 03760_view1;
DROP VIEW 03760_mview1;

-- Verify dropped views are no longer present in dependencies
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src1';
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src2';

DROP TABLE 03760_src2;
DROP TABLE 03760_src1;
EOF

# RENAME DATABASE section
$CLICKHOUSE_CLIENT <<EOF
-- RENAME DATABASE: plain-view dependencies must be migrated to the new database name.
-- Three cases are exercised:
--   (a) single-source plain view: v_simple reads from src1 (same DB, renamed together)
--   (b) multi-source plain view: v_join reads from src1 JOIN src2 (same DB, renamed together)
--   (c) cross-db plain view: v_xdb lives in an external DB and reads from src1 (source DB renamed)
CREATE DATABASE ${DB_RENAME_BEFORE} ENGINE = Atomic;
CREATE TABLE ${DB_RENAME_BEFORE}.src1 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ${DB_RENAME_BEFORE}.src2 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB_RENAME_BEFORE}.v_simple AS SELECT * FROM ${DB_RENAME_BEFORE}.src1;
CREATE VIEW ${DB_RENAME_BEFORE}.v_join   AS SELECT a.id FROM ${DB_RENAME_BEFORE}.src1 a JOIN ${DB_RENAME_BEFORE}.src2 b ON a.id = b.id;

CREATE DATABASE ${DB_RENAME_EXT} ENGINE = Atomic;
CREATE VIEW ${DB_RENAME_EXT}.v_xdb AS SELECT * FROM ${DB_RENAME_BEFORE}.src1;

-- Before rename: verify all three views appear as dependents.
SELECT arraySort(arrayMap((x, y) -> concat(x, '.', y), dependencies_database, dependencies_table))
FROM system.tables WHERE database = '${DB_RENAME_BEFORE}' AND name = 'src1';
SELECT arraySort(dependencies_table)
FROM system.tables WHERE database = '${DB_RENAME_BEFORE}' AND name = 'src2';

RENAME DATABASE ${DB_RENAME_BEFORE} TO ${DB_RENAME_AFTER};

-- After rename: the same three views must still be reported under the new DB name.
-- v_simple and v_join have been renamed together with their source; v_xdb stays in its own DB.
SELECT arraySort(arrayMap((x, y) -> concat(x, '.', y), dependencies_database, dependencies_table))
FROM system.tables WHERE database = '${DB_RENAME_AFTER}' AND name = 'src1';
SELECT arraySort(dependencies_table)
FROM system.tables WHERE database = '${DB_RENAME_AFTER}' AND name = 'src2';

DROP DATABASE ${DB_RENAME_AFTER};
DROP DATABASE ${DB_RENAME_EXT};
EOF

$CLICKHOUSE_CLIENT <<EOF
-- DROP source table must remove plain_view_dependencies edges where it appears as source.
DROP TABLE IF EXISTS 03760_stale_src;
DROP VIEW  IF EXISTS 03760_stale_view;

CREATE TABLE 03760_stale_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW 03760_stale_view AS SELECT * FROM 03760_stale_src;

SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_stale_src';

DROP VIEW 03760_stale_view;
DROP TABLE 03760_stale_src;

-- Recreate the source table under the same name (no view exists any more).
CREATE TABLE 03760_stale_src (id UInt64) ENGINE = MergeTree ORDER BY id;

-- The dropped view must not appear as a dependent of the new table.
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_stale_src';

DROP TABLE 03760_stale_src;

-- RENAME TABLE (source): plain_view_dependencies outgoing edges must be re-added under the new name.
DROP TABLE IF EXISTS 03760_rename_src;
DROP TABLE IF EXISTS 03760_rename_src2;
DROP VIEW  IF EXISTS 03760_rename_view;

CREATE TABLE 03760_rename_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW 03760_rename_view AS SELECT * FROM 03760_rename_src;

SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_rename_src';

RENAME TABLE 03760_rename_src TO 03760_rename_src2;

-- After rename, 03760_rename_src2 must have the same plain view dependency.
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_rename_src2';

DROP VIEW 03760_rename_view;
DROP TABLE 03760_rename_src2;

-- CREATE OR REPLACE TABLE (source): dependent plain views must survive the implicit EXCHANGE.
DROP TABLE IF EXISTS 03760_cor_src;
DROP VIEW  IF EXISTS 03760_cor_view;

CREATE TABLE 03760_cor_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW 03760_cor_view AS SELECT * FROM 03760_cor_src;

SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_cor_src';

CREATE OR REPLACE TABLE 03760_cor_src (id UInt64, extra String) ENGINE = MergeTree ORDER BY id;

SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_cor_src';

DROP VIEW 03760_cor_view;
DROP TABLE 03760_cor_src;

-- DETACH VIEW (non-permanent): must clean up plain_view_dependencies edges on the source.
DROP TABLE IF EXISTS 03760_det_src;
DROP VIEW  IF EXISTS 03760_det_view;

CREATE TABLE 03760_det_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW 03760_det_view AS SELECT * FROM 03760_det_src;

SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_det_src';

DETACH VIEW 03760_det_view;

SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_det_src';

DROP TABLE 03760_det_src;
EOF
