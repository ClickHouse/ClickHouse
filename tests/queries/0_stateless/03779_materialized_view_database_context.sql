-- Tags: no-parallel

-- Test that materialized views use the correct database context when calling
-- parameterized views (issue #87082)

-- Setup db1 with table, parameterized view, and REFRESH MV
DROP DATABASE IF EXISTS db1_03779;
DROP DATABASE IF EXISTS db2_03779;

CREATE DATABASE db1_03779;
USE db1_03779;

CREATE TABLE tbl1 (id Int32, data String) ENGINE = MergeTree ORDER BY id;
INSERT INTO tbl1 VALUES (1, 'db1_data');

-- Parameterized view in db1_03779
CREATE VIEW v1 AS SELECT id, data, {p:String} as p FROM tbl1;

-- REFRESH MV calling unqualified parameterized view (should use db1_03779 context)
CREATE MATERIALIZED VIEW mv1
REFRESH EVERY 1 SECOND
ORDER BY id
AS SELECT id, data, p FROM v1(p='from_mv1_db1');

-- Wait for the REFRESH MV to complete first refresh
SYSTEM WAIT VIEW db1_03779.mv1;

-- Query the MV - should have data from db1_03779
SELECT 'mv1_db1:', * FROM db1_03779.mv1 ORDER BY id;

-- Setup db2_03779 with same structure but different data
CREATE DATABASE db2_03779;
USE db2_03779;

CREATE TABLE tbl1 (id Int32, data String) ENGINE = MergeTree ORDER BY id;
INSERT INTO tbl1 VALUES (2, 'db2_data');

-- Parameterized view in db2_03779 (same name as db1_03779.v1)
CREATE VIEW v1 AS SELECT id, data, {p:String} as p FROM tbl1;

-- REFRESH MV calling unqualified parameterized view (should use db2_03779 context)
CREATE MATERIALIZED VIEW mv1
REFRESH EVERY 1 SECOND
ORDER BY id
AS SELECT id, data, p FROM v1(p='from_mv1_db2');

-- Wait for db2 MV to complete first refresh
SYSTEM WAIT VIEW db2_03779.mv1;

-- Should get data from db2_03779, not db1_03779
SELECT 'mv1_db2:', * FROM db2_03779.mv1 ORDER BY id;

-- Test that qualified references still work
USE db1_03779;

CREATE MATERIALIZED VIEW mv_qualified_crossdb
REFRESH EVERY 1 SECOND
ORDER BY id
AS SELECT id, data, p FROM db2_03779.v1(p='qualified_crossdb');

SYSTEM WAIT VIEW db1_03779.mv_qualified_crossdb;
SELECT 'qualified:', * FROM db1_03779.mv_qualified_crossdb ORDER BY id;

-- Cleanup
DROP DATABASE db1_03779;
DROP DATABASE db2_03779;
