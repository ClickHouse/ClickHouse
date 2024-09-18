-- Tags: no-parallel

SELECT * FROM loop(numbers(3)) LIMIT 10;
SELECT * FROM loop (numbers(3)) LIMIT 10 settings max_block_size = 1;

DROP DATABASE IF EXISTS 03147_db;
CREATE DATABASE IF NOT EXISTS 03147_db;
CREATE TABLE 03147_db.t (n Int8) ENGINE=MergeTree ORDER BY n;
INSERT INTO 03147_db.t SELECT * FROM numbers(10);
USE 03147_db;

SELECT * FROM loop(03147_db.t) LIMIT 15;
SELECT * FROM loop(t) LIMIT 15;
SELECT * FROM loop(03147_db, t) LIMIT 15;

SELECT * FROM loop('', '') -- { serverError UNKNOWN_TABLE }
