DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table (id UInt64, value UInt64) ENGINE=MergeTree ORDER BY (id);
SYSTEM STOP MERGES test_table;

INSERT INTO test_table SELECT number % 15, number FROM numbers_mt(500000000);
SYSTEM START MERGES test_table;

OPTIMIZE TABLE test_table;
