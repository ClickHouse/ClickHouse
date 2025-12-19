-- Tags: no-parallel

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

-- 03760_src1 should show 03760_view1, 03760_view2, 03760_view3, and 03760_mview1 as dependents
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

-- Cross-database
CREATE DATABASE IF NOT EXISTS db_03760_x;
CREATE TABLE db_03760_x.remote_t (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW 03760_local_view_of_x AS SELECT * FROM db_03760_x.remote_t;
CREATE VIEW db_03760_x.remote_view_of_local AS SELECT * FROM 03760_src1;

-- 03760_src1 should list db_03760_x.remote_view_of_local among dependents (view in other db depends on current db table)
SELECT concat(dependencies_database, '.', dependencies_table) AS dep
FROM system.tables
ARRAY JOIN dependencies_database, dependencies_table
WHERE database = currentDatabase() AND name = '03760_src1'
ORDER BY dep;

-- db_03760_x.remote_t should list 03760_local_view_of_x as dependent (view in current db depends on other db table)
SELECT DISTINCT dependencies_table AS dep
FROM system.tables
ARRAY JOIN dependencies_table
WHERE database = 'db_03760_x' AND name = 'remote_t'
ORDER BY dep;

DROP VIEW db_03760_x.remote_view_of_local;
DROP VIEW 03760_local_view_of_x;
DROP TABLE db_03760_x.remote_t;
DROP DATABASE db_03760_x;

DROP VIEW 03760_view3;
DROP VIEW 03760_view2;
DROP VIEW 03760_view1;
DROP VIEW 03760_mview1;

-- Verify dropped views are no longer present in dependencies
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src1';
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src2';

DROP TABLE 03760_src2;
DROP TABLE 03760_src1;
