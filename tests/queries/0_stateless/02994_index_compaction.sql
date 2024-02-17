DROP TABLE IF EXISTS test;
CREATE TABLE test (t UInt64, s String) ENGINE = MergeTree ORDER BY (t, s) SETTINGS index_granularity = 1;

INSERT INTO test SELECT rand64(), randomString(1000) FROM numbers(100000);
SELECT round(primary_key_bytes_in_memory, -6), round(primary_key_bytes_in_memory_allocated, -6) FROM system.parts WHERE database = currentDatabase() AND table = 'test';

-- The index is being compacted only after loading parts from disk, not on creation of zero-level parts.
DETACH TABLE test;
ATTACH TABLE test;

SELECT t >= 0 FROM test LIMIT 1;

SELECT round(primary_key_bytes_in_memory, -6), round(primary_key_bytes_in_memory_allocated, -6) FROM system.parts WHERE database = currentDatabase() AND table = 'test';

DROP TABLE test;
