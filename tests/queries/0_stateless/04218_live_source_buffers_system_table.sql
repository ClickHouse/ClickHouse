-- Test system.live_source_buffers schema and basic read through ReaderExecutor.

SELECT name, type
FROM system.columns
WHERE database = 'system' AND table = 'live_source_buffers'
ORDER BY position;

SELECT count() >= 0 FROM system.live_source_buffers;

-- Read data through ReaderExecutor and verify correctness.
DROP TABLE IF EXISTS test_live_source_buffers;
CREATE TABLE test_live_source_buffers (x UInt64) ENGINE = MergeTree ORDER BY x;
SET use_reader_executor = 0;
INSERT INTO test_live_source_buffers SELECT number FROM numbers(10000);

SELECT sum(x) FROM test_live_source_buffers SETTINGS use_reader_executor = 1;

DROP TABLE test_live_source_buffers;
