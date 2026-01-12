-- Test that Materialized Views are triggered when inserting through Alias tables

DROP TABLE IF EXISTS source_table;
DROP TABLE IF EXISTS alias_table;
DROP TABLE IF EXISTS mv_target;
DROP TABLE IF EXISTS mv;

-- Create source table
CREATE TABLE source_table (id UInt32, value String) ENGINE = MergeTree ORDER BY id;

-- Create MV target table and MV that triggers on source_table inserts
CREATE TABLE mv_target (id UInt32, value String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv TO mv_target AS SELECT id, value FROM source_table;

-- Create alias to source_table
CREATE TABLE alias_table ENGINE = Alias('source_table') settings allow_experimental_alias_table_engine=1;

-- Test 1: Direct insert to source_table triggers MV
SELECT 'Direct insert to source_table';
INSERT INTO source_table VALUES (1, 'direct');
SELECT 'source_table count:', count() FROM source_table;
SELECT 'mv_target count:', count() FROM mv_target;

-- Test 2: Insert through alias also triggers MV
SELECT 'Insert through alias_table';
INSERT INTO alias_table VALUES (2, 'via_alias');
SELECT 'source_table count:', count() FROM source_table;
SELECT 'mv_target count:', count() FROM mv_target;

DROP TABLE mv;
DROP TABLE mv_target;
DROP TABLE alias_table;
DROP TABLE source_table;
