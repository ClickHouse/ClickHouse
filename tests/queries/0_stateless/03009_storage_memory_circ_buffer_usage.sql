-- Tags: memory-engine
SET max_block_size = 65409; -- Default value

DROP TABLE IF EXISTS memory;
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_bytes_to_keep = 4096, max_bytes_to_keep = 16384;

SELECT 'TESTING BYTES';
/* 1. testing oldest block doesn't get deleted because of min-threshold */
INSERT INTO memory SELECT * FROM numbers(0, 1600);
SELECT total_bytes FROM system.tables WHERE name = 'memory' and database = currentDatabase();

/* 2. adding block that doesn't get deleted */
INSERT INTO memory SELECT * FROM numbers(1000, 100);
SELECT total_bytes FROM system.tables WHERE name = 'memory' and database = currentDatabase();

/* 3. testing oldest block gets deleted - 9216 bytes - 1100 */
INSERT INTO memory SELECT * FROM numbers(9000, 1000);
SELECT total_bytes FROM system.tables WHERE name = 'memory' and database = currentDatabase();

/* 4.check large block over-writes all bytes / rows */
INSERT INTO memory SELECT * FROM numbers(9000, 10000);
SELECT total_bytes FROM system.tables WHERE name = 'memory' and database = currentDatabase();

DROP TABLE IF EXISTS memory;
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 100, max_rows_to_keep = 1000;

SELECT 'TESTING ROWS';
/* 1. add normal number of rows */
INSERT INTO memory SELECT * FROM numbers(0, 50);
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase();

/* 2. table should have 1000 */
INSERT INTO memory SELECT * FROM numbers(50, 950);
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase();

/* 3. table should have 1020 - removed first 50 */
INSERT INTO memory SELECT * FROM numbers(2000, 70);
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase();

/* 4. check large block over-writes all rows */
INSERT INTO memory SELECT * FROM numbers(3000, 1100);
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase();

SELECT 'TESTING NO CIRCULAR-BUFFER';
DROP TABLE IF EXISTS memory;
CREATE TABLE memory (i UInt32) ENGINE = Memory;

INSERT INTO memory SELECT * FROM numbers(0, 1600);
SELECT total_bytes FROM system.tables WHERE name = 'memory' and database = currentDatabase();

INSERT INTO memory SELECT * FROM numbers(1000, 100);
SELECT total_bytes FROM system.tables WHERE name = 'memory' and database = currentDatabase();

INSERT INTO memory SELECT * FROM numbers(9000, 1000);
SELECT total_bytes FROM system.tables WHERE name = 'memory' and database = currentDatabase();

INSERT INTO memory SELECT * FROM numbers(9000, 10000);
SELECT total_bytes FROM system.tables WHERE name = 'memory' and database = currentDatabase();

SELECT 'TESTING INVALID SETTINGS';
CREATE TABLE faulty_memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 100;  -- { serverError SETTING_CONSTRAINT_VIOLATION }
CREATE TABLE faulty_memory (i UInt32) ENGINE = Memory SETTINGS min_bytes_to_keep = 100; -- { serverError SETTING_CONSTRAINT_VIOLATION }

DROP TABLE memory;
