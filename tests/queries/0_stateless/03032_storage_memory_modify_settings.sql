SET max_block_size = 65409; -- Default value

SELECT 'TESTING MODIFY SMALLER BYTES';
DROP TABLE IF EXISTS memory;
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_bytes_to_keep = 8192, max_bytes_to_keep = 32768;

INSERT INTO memory SELECT * FROM numbers(0, 100); -- 1024 bytes
INSERT INTO memory SELECT * FROM numbers(0, 3000); -- 16384 bytes
SELECT total_bytes FROM system.tables WHERE name = 'memory' and database = currentDatabase(); -- 17408 in total

ALTER TABLE memory MODIFY SETTING min_bytes_to_keep = 4096, max_bytes_to_keep = 16384;
SELECT total_bytes FROM system.tables WHERE name = 'memory' and database = currentDatabase(); -- 16384 in total after deleting

INSERT INTO memory SELECT * FROM numbers(3000, 10000); -- 65536 bytes
SELECT total_bytes FROM system.tables WHERE name = 'memory' and database = currentDatabase();

SELECT 'TESTING MODIFY SMALLER ROWS';
DROP TABLE IF EXISTS memory;
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 200, max_rows_to_keep = 2000;

INSERT INTO memory SELECT * FROM numbers(0, 100); -- 100 rows
INSERT INTO memory SELECT * FROM numbers(100, 1000); -- 1000 rows
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase(); -- 1100 in total

ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100, max_rows_to_keep = 1000;
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase(); -- 1000 in total after deleting

INSERT INTO memory SELECT * FROM numbers(1000, 500); -- 500 rows
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase(); -- 500 in total after deleting

SELECT 'TESTING ADD SETTINGS';
DROP TABLE IF EXISTS memory;
CREATE TABLE memory (i UInt32) ENGINE = Memory;

INSERT INTO memory SELECT * FROM numbers(0, 50); -- 50 rows
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase(); -- 50 in total

INSERT INTO memory SELECT * FROM numbers(50, 950); -- 950 rows
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase(); -- 1000 in total

INSERT INTO memory SELECT * FROM numbers(2000, 70); -- 70 rows
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase(); -- 1070 in total

ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100, max_rows_to_keep = 1000;
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase(); -- 1020 in total after deleting

INSERT INTO memory SELECT * FROM numbers(3000, 1100); -- 1100 rows
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase(); -- 1100 in total after deleting

SELECT 'TESTING ADD SETTINGS';
DROP TABLE IF EXISTS memory;
CREATE TABLE memory (i UInt32) ENGINE = Memory;
ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100, max_rows_to_keep = 1000;

INSERT INTO memory SELECT * FROM numbers(0, 50); -- 50 rows
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase(); -- 50 in total

INSERT INTO memory SELECT * FROM numbers(50, 950); -- 950 rows
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase(); -- 1000 in total

INSERT INTO memory SELECT * FROM numbers(2000, 70); -- 70 rows
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase(); -- 1020 in total after deleting

INSERT INTO memory SELECT * FROM numbers(3000, 1100); -- 1100 rows
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase(); -- 1100 in total after deleting

SELECT 'TESTING INVALID SETTINGS';
DROP TABLE IF EXISTS memory;
CREATE TABLE memory (i UInt32) ENGINE = Memory;
ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100;  -- { serverError SETTING_CONSTRAINT_VIOLATION }
ALTER TABLE memory MODIFY SETTING min_bytes_to_keep = 100; -- { serverError SETTING_CONSTRAINT_VIOLATION }
ALTER TABLE memory MODIFY SETTING max_rows_to_keep = 1000;
ALTER TABLE memory MODIFY SETTING max_bytes_to_keep = 1000;

DROP TABLE memory;

