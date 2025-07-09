DROP TABLE IF EXISTS test;
CREATE TABLE test (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;

SET optimize_trivial_insert_select = 1;
INSERT INTO test SELECT randomString(1000) FROM numbers(100000);
SELECT round(primary_key_bytes_in_memory, -7), round(primary_key_bytes_in_memory_allocated, -7) FROM system.parts WHERE database = currentDatabase() AND table = 'test';

DETACH TABLE test;
SET max_memory_usage = '50M';
ATTACH TABLE test;

SELECT primary_key_bytes_in_memory, primary_key_bytes_in_memory_allocated FROM system.parts WHERE database = currentDatabase() AND table = 'test';

SET max_memory_usage = '200M';
SELECT s != '' FROM test LIMIT 1;

SELECT round(primary_key_bytes_in_memory, -7), round(primary_key_bytes_in_memory_allocated, -7) FROM system.parts WHERE database = currentDatabase() AND table = 'test';

DROP TABLE test;
