DROP TABLE IF EXISTS test.index_memory;
CREATE TABLE test.index_memory (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO test.index_memory SELECT * FROM system.numbers LIMIT 10000000;
SELECT count() FROM test.index_memory;
DETACH TABLE test.index_memory;
SET max_memory_usage = 79000000;
ATTACH TABLE test.index_memory;
SELECT count() FROM test.index_memory;
DROP TABLE test.index_memory;
