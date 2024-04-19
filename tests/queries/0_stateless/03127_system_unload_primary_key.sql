DROP TABLE IF EXISTS test;
CREATE TABLE test (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;

INSERT INTO test SELECT randomString(1000) FROM numbers(100000);

SELECT round(primary_key_bytes_in_memory, -7), round(primary_key_bytes_in_memory_allocated, -7) FROM system.parts WHERE database = currentDatabase() AND table = 'test';

SYSTEM UNLOAD PRIMARY KEY {CLICKHOUSE_DATABASE:Identifier}.test;

SELECT primary_key_bytes_in_memory, primary_key_bytes_in_memory_allocated FROM system.parts WHERE database = currentDatabase() AND table = 'test';

DROP TABLE IF EXISTS test2;
CREATE TABLE test2 (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;

INSERT INTO test2 SELECT randomString(1000) FROM numbers(100000);

SELECT round(primary_key_bytes_in_memory, -7), round(primary_key_bytes_in_memory_allocated, -7) FROM system.parts WHERE database = currentDatabase() AND table IN ('test', 'test2');

SELECT s != '' FROM test LIMIT 1;

SELECT round(primary_key_bytes_in_memory, -7), round(primary_key_bytes_in_memory_allocated, -7) FROM system.parts WHERE database = currentDatabase() AND table IN ('test', 'test2');

SYSTEM UNLOAD PRIMARY KEY;

SELECT primary_key_bytes_in_memory, primary_key_bytes_in_memory_allocated FROM system.parts WHERE database = currentDatabase() AND table IN ('test', 'test2');
