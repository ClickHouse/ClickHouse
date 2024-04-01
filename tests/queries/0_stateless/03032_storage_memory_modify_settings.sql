SET max_block_size = 65409; -- Default value

SELECT 'TESTING MODIFY SMALLER BYTES';
DROP TABLE IF EXISTS memory;
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_bytes_to_keep = 8192, max_bytes_to_keep = 32768;

INSERT INTO memory SELECT * FROM numbers(0, 100);
INSERT INTO memory SELECT * FROM numbers(0, 3000);
SELECT total_bytes FROM system.tables WHERE name = 'memory' and database = currentDatabase();

ALTER TABLE memory MODIFY SETTING min_bytes_to_keep = 4096, max_bytes_to_keep = 16384;
SELECT total_bytes FROM system.tables WHERE name = 'memory' and database = currentDatabase();

INSERT INTO memory SELECT * FROM numbers(3000, 10000);
SELECT total_bytes FROM system.tables WHERE name = 'memory' and database = currentDatabase();

SELECT 'TESTING MODIFY SMALLER ROWS';
DROP TABLE IF EXISTS memory;
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 200, max_rows_to_keep = 2000;

INSERT INTO memory SELECT * FROM numbers(0, 100);
INSERT INTO memory SELECT * FROM numbers(100, 1000);
SELECT total_bytes FROM system.tables WHERE name = 'memory' and database = currentDatabase();

ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100, max_rows_to_keep = 1000;
SELECT total_bytes FROM system.tables WHERE name = 'memory' and database = currentDatabase();

INSERT INTO memory SELECT * FROM numbers(1000, 500);
SELECT total_bytes FROM system.tables WHERE name = 'memory' and database = currentDatabase();

SELECT 'TESTING ADD SETTINGS';
DROP TABLE IF EXISTS memory;
CREATE TABLE memory (i UInt32) ENGINE = Memory;

INSERT INTO memory SELECT * FROM numbers(0, 50);
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase();

INSERT INTO memory SELECT * FROM numbers(50, 950);
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase();

INSERT INTO memory SELECT * FROM numbers(2000, 70);
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase();

ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100, max_rows_to_keep = 1000;
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase();

INSERT INTO memory SELECT * FROM numbers(3000, 1100);
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase();

SELECT 'TESTING ADD SETTINGS';
DROP TABLE IF EXISTS memory;
CREATE TABLE memory (i UInt32) ENGINE = Memory;
ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100, max_rows_to_keep = 1000;

INSERT INTO memory SELECT * FROM numbers(0, 50);
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase();

INSERT INTO memory SELECT * FROM numbers(50, 950);
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase();

INSERT INTO memory SELECT * FROM numbers(2000, 70);
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase();

INSERT INTO memory SELECT * FROM numbers(3000, 1100);
SELECT total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase();

SELECT 'TESTING INVALID SETTINGS';
DROP TABLE IF EXISTS memory;
CREATE TABLE memory (i UInt32) ENGINE = Memory;
ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100;  -- { serverError 452 }
ALTER TABLE memory MODIFY SETTING min_bytes_to_keep = 100; -- { serverError 452 }

DROP TABLE memory;