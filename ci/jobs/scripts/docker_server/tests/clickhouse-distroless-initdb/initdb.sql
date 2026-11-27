CREATE DATABASE IF NOT EXISTS test_db;
CREATE TABLE IF NOT EXISTS test_db.test_table (id UInt32, value UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_db.test_table VALUES (1, 100), (2, 200);
