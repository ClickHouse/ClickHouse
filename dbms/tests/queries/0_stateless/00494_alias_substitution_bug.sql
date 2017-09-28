CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.nested;
CREATE TABLE test.nested (n Nested(x UInt8)) ENGINE = Memory;
INSERT INTO test.nested VALUES ([1, 2]);
SELECT 1 AS x FROM remote('127.0.0.1', test.nested) ARRAY JOIN n.x;
DROP TABLE test.nested;
