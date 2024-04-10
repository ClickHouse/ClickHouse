DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table  (number UInt64) ENGINE=MergeTree ORDER BY number;
INSERT INTO test_table SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table;
DROP TABLE test_table;

CREATE TABLE test_table  (number UInt64) ENGINE=MergeTree ORDER BY number;
INSERT INTO test_table SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table PERMANENTLY;
DROP TABLE test_table;