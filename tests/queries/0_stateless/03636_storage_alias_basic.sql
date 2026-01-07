-- { echo }
-- Tags: long

DROP TABLE IF EXISTS source_table;
DROP TABLE IF EXISTS alias_1;
DROP TABLE IF EXISTS alias_2;
DROP TABLE IF EXISTS alias_3;
DROP TABLE IF EXISTS alias_4;

SET allow_experimental_alias_table_engine = 1;

-- Create source table
CREATE TABLE source_table (id UInt32, value String) ENGINE = MergeTree ORDER BY id;

INSERT INTO source_table VALUES (1, 'one'), (2, 'two'), (3, 'three');

-- Test: Basic alias creation
SELECT 'Test Basic alias creation';
CREATE TABLE alias_1 ENGINE = Alias('source_table');
SELECT * FROM alias_1 ORDER BY id;

-- Test: Alias with database name
SELECT 'Test Alias with database name';
CREATE TABLE alias_2 ENGINE = Alias(currentDatabase(), 'source_table');
SELECT * FROM alias_2 ORDER BY id;

-- Test: Insert through alias
SELECT 'Test Insert through alias_1';
INSERT INTO alias_1 VALUES (4, 'four');
SELECT * FROM source_table ORDER BY id;

-- Test: Insert through alias_2
SELECT 'Test Insert through alias_2';
INSERT INTO alias_2 VALUES (5, 'five');
SELECT * FROM source_table ORDER BY id;

-- Test: ALTER ADD COLUMN
SELECT 'Test ALTER ADD COLUMN';
ALTER TABLE alias_1 ADD COLUMN status String DEFAULT 'active';
SELECT id, value, status FROM source_table ORDER BY id;

-- Test: INSERT with new column
SELECT 'Test Insert with new column';
INSERT INTO alias_1 VALUES (6, 'six', 'inactive');
SELECT * FROM source_table ORDER BY id;

-- Test: Truncate
SELECT 'Test TRUNCATE';
TRUNCATE TABLE alias_1;
SELECT count() FROM source_table;

-- Re-insert data
INSERT INTO source_table VALUES (1, 'one', 'active'), (2, 'two', 'active');

-- Test: Rename alias
SELECT 'Test RENAME alias';
RENAME TABLE alias_1 TO alias_3;
SELECT * FROM alias_3 ORDER BY id;

-- Test: Drop alias (should not affect source table)
SELECT 'Test DROP alias';
DROP TABLE alias_2;
DROP TABLE alias_3;
SELECT count() FROM source_table;

-- Test: Create alias
SELECT 'Test Create alias';
CREATE TABLE alias_4 ENGINE = Alias('source_table');
SELECT * FROM alias_4 ORDER BY id;

-- Test: OPTIMIZE through alias
SELECT 'Test OPTIMIZE';
INSERT INTO alias_4 VALUES (10, 'ten', 'active');
INSERT INTO alias_4 VALUES (11, 'eleven', 'active');
INSERT INTO alias_4 VALUES (12, 'twelve', 'active');
OPTIMIZE TABLE alias_4 FINAL;
SELECT count() AS parts_after FROM system.parts
WHERE database = currentDatabase() AND table = 'source_table' AND active;
SELECT count() FROM alias_4;

-- Test: ALTER MODIFY SETTING
SELECT 'Test ALTER MODIFY SETTING';
ALTER TABLE alias_4 MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 1000000;
SHOW CREATE TABLE source_table FORMAT TSVRaw;

-- Test: UPDATE through alias
SELECT 'Test UPDATE (mutate)';
ALTER TABLE alias_4 UPDATE value = 'updated' WHERE id = 1 SETTINGS mutations_sync = 1;
SELECT id, value, status FROM source_table WHERE id = 1;

-- Test: DELETE through alias
SELECT 'Test DELETE (mutate)';
ALTER TABLE alias_4 DELETE WHERE id = 2 SETTINGS mutations_sync = 1;
SELECT count() FROM source_table WHERE id = 2;

-- Test: Partition operations
SELECT 'Test Partition operations';
DROP TABLE IF EXISTS source_partitioned;
DROP TABLE IF EXISTS alias_part;
CREATE TABLE source_partitioned (date Date, id UInt32, value String)
    ENGINE = MergeTree PARTITION BY toYYYYMM(date) ORDER BY id;
CREATE TABLE alias_part ENGINE = Alias('source_partitioned');
INSERT INTO alias_part VALUES ('2024-01-15', 1, 'january'), ('2024-02-15', 2, 'february'), ('2024-03-15', 3, 'march');
SELECT count() FROM alias_part;

-- Test: DETACH PARTITION
SELECT 'Test DETACH PARTITION';
ALTER TABLE alias_part DETACH PARTITION '202401';
SELECT count() FROM alias_part;

-- Test: ATTACH PARTITION
SELECT 'Test ATTACH PARTITION';
ALTER TABLE alias_part ATTACH PARTITION '202401';
SELECT count() FROM alias_part;

-- Test: DROP PARTITION
SELECT 'Test DROP PARTITION';
ALTER TABLE alias_part DROP PARTITION '202403';
SELECT count() FROM alias_part;

-- Test: INSERT SELECT
SELECT 'Test INSERT SELECT';
INSERT INTO alias_4 SELECT id + 100, value, status FROM source_table WHERE id <= 10;
SELECT count() FROM source_table WHERE id > 100;

-- Test: EXCHANGE TABLES
DROP TABLE IF EXISTS table_a_exchange;
DROP TABLE IF EXISTS table_b_exchange;
DROP TABLE IF EXISTS alias_a_exchange;
DROP TABLE IF EXISTS alias_b_exchange;

CREATE TABLE table_a_exchange (value String) ENGINE = MergeTree() ORDER BY value;
CREATE TABLE table_b_exchange (value String) ENGINE = MergeTree() ORDER BY value;

INSERT INTO table_a_exchange VALUES ('from_a');
INSERT INTO table_b_exchange VALUES ('from_b');

CREATE TABLE alias_a_exchange ENGINE = Alias(table_a_exchange);
CREATE TABLE alias_b_exchange ENGINE = Alias(table_b_exchange);

SELECT 'Before EXCHANGE';
SELECT * FROM alias_a_exchange ORDER BY value;
SELECT * FROM alias_b_exchange ORDER BY value;

-- EXCHANGE the alias
EXCHANGE TABLES alias_a_exchange AND alias_b_exchange;

SELECT 'After EXCHANGE alias tables';
SELECT * FROM alias_a_exchange ORDER BY value;  -- Should show 'from_b'
SELECT * FROM alias_b_exchange ORDER BY value;  -- Should show 'from_a'

-- EXCHANGE the source tables
EXCHANGE TABLES table_a_exchange AND table_b_exchange;

SELECT 'After EXCHANGE source tables';
SELECT * FROM alias_a_exchange ORDER BY value;  -- Should show 'from_a'
SELECT * FROM alias_b_exchange ORDER BY value;  -- Should show 'from_b'

DROP TABLE alias_a_exchange;
DROP TABLE alias_b_exchange;
DROP TABLE table_a_exchange;
DROP TABLE table_b_exchange;

-- Test: DETACH and ATTACH TABLE
SELECT 'Test DETACH and ATTACH TABLE';
DROP TABLE IF EXISTS source_attach;
DROP TABLE IF EXISTS alias_attach;

CREATE TABLE source_attach (id UInt32, data String) ENGINE = MergeTree ORDER BY id;
INSERT INTO source_attach VALUES (1, 'data1'), (2, 'data2');

CREATE TABLE alias_attach ENGINE = Alias('source_attach');
SELECT * FROM alias_attach ORDER BY id;
SELECT * FROM source_attach ORDER BY id;

-- DETACH the alias table
DETACH TABLE alias_attach;

-- ATTACH the table back
ATTACH TABLE alias_attach;

-- Verify it works after ATTACH
SELECT 'After ATTACH';
SELECT * FROM alias_attach ORDER BY id;
SELECT * FROM source_attach ORDER BY id;

-- Insert through alias after ATTACH
INSERT INTO alias_attach VALUES (3, 'data3');
SELECT * FROM alias_attach ORDER BY id;
SELECT * FROM source_attach ORDER BY id;

DROP TABLE alias_attach;
DROP TABLE source_attach;

-- Test: Circular reference check
SELECT 'Test circular reference prevention';
CREATE TABLE self_ref_test ENGINE = Alias('self_ref_test'); -- { serverError BAD_ARGUMENTS }

SELECT 'Test ALTER target directly';
CREATE TABLE metadata_target (id UInt32, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE metadata_alias ENGINE = Alias('metadata_target');

INSERT INTO metadata_target VALUES (1, 'one'), (2, 'two');
SELECT * FROM metadata_alias ORDER BY id;

-- ALTER target table DIRECTLY (not through alias)
ALTER TABLE metadata_target ADD COLUMN extra String DEFAULT 'data1';

-- Alias should immediately see the new column (dynamic metadata fetch)
SELECT 'After ALTER target';
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'metadata_alias' ORDER BY name;

-- INSERT through alias with new column should work
INSERT INTO metadata_alias VALUES (3, 'three', 'data3');
SELECT * FROM metadata_alias ORDER BY id;

ALTER TABLE metadata_target ADD COLUMN num UInt32 DEFAULT 0;
ALTER TABLE metadata_target MODIFY COLUMN extra String DEFAULT 'data2';

SELECT 'After multiple ALTERs';
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'metadata_alias' ORDER BY name;

-- Insert with all columns
INSERT INTO metadata_alias VALUES (4, 'four', DEFAULT, 100);
SELECT id, value, extra, num FROM metadata_alias WHERE id = 4;

-- DROP COLUMN on target
ALTER TABLE metadata_target DROP COLUMN num;

-- Alias should not show dropped column
SELECT 'After DROP';
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'metadata_alias' ORDER BY name;

-- INSERT after DROP should work (without dropped column)
INSERT INTO metadata_alias VALUES (5, 'five', 'data5');
SELECT id, value, extra FROM metadata_alias WHERE id = 5;

DROP TABLE metadata_alias;
DROP TABLE metadata_target;

SELECT 'Test alias with missing target table';
DROP TABLE IF EXISTS alias_with_missing_target;
DROP TABLE IF EXISTS temp_target;

CREATE TABLE temp_target (id UInt32, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO temp_target VALUES (1, 'data1'), (2, 'data2');

CREATE TABLE alias_with_missing_target ENGINE = Alias('temp_target');
SELECT * FROM alias_with_missing_target ORDER BY id;

DROP TABLE temp_target;

SELECT name, engine FROM system.tables WHERE database = currentDatabase() AND name = 'alias_with_missing_target';

SELECT arraySort(groupUniqArray(name)) FROM system.tables WHERE database = currentDatabase();
SELECT arraySort(groupUniqArray(name)) FROM system.columns WHERE database = currentDatabase() AND table = 'alias_with_missing_target';

SELECT database, table, name FROM system.columns
WHERE database = currentDatabase() AND table = 'alias_with_missing_target'
ORDER BY name;

SELECT name, engine, total_rows, total_bytes, data_paths
FROM system.tables
WHERE database = currentDatabase() AND name = 'alias_with_missing_target';

DROP TABLE alias_with_missing_target;