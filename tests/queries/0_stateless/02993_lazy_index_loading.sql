DROP TABLE IF EXISTS test;
CREATE TABLE test (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1, use_primary_key_cache = 0;

SET optimize_trivial_insert_select = 1;
INSERT INTO test SELECT randomString(1000) FROM numbers(100000);
SELECT round(primary_key_bytes_in_memory, -7), round(primary_key_bytes_in_memory_allocated, -7) FROM system.parts WHERE database = currentDatabase() AND table = 'test' FORMAT Vertical;

DETACH TABLE test;
SET max_memory_usage = '50M';
ATTACH TABLE test;

SELECT primary_key_bytes_in_memory, primary_key_bytes_in_memory_allocated FROM system.parts WHERE database = currentDatabase() AND table = 'test' FORMAT Vertical;

SET max_memory_usage = '200M';

-- Run a query that doesn use indexes
SELECT s != '' FROM test LIMIT 1;

-- Check that index was not loaded
SELECT primary_key_bytes_in_memory, primary_key_bytes_in_memory_allocated FROM system.parts WHERE database = currentDatabase() AND table = 'test' FORMAT Vertical;

-- Run a query that uses PK index
SET max_execution_time = 300;
SELECT s != '' FROM test WHERE s < '9999999999' LIMIT 1;

-- Check that index was loaded
SELECT round(primary_key_bytes_in_memory, -7), round(primary_key_bytes_in_memory_allocated, -7) FROM system.parts WHERE database = currentDatabase() AND table = 'test' FORMAT Vertical;

DROP TABLE test;
