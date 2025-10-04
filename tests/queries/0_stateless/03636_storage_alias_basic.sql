DROP TABLE IF EXISTS source_table;
DROP TABLE IF EXISTS alias_1;
DROP TABLE IF EXISTS alias_2;
DROP TABLE IF EXISTS alias_3;
DROP TABLE IF EXISTS alias_4;
DROP TABLE IF EXISTS source_other;

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

-- Test: Explicit columns
SELECT 'Test Alias with explicit columns';
CREATE TABLE alias_4 (id UInt32, value String, status String) ENGINE = Alias('source_table');
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
