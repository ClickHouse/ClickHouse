-- Test system.remote_read_connections schema and basic read through ReaderExecutor.

SELECT name, type
FROM system.columns
WHERE database = 'system' AND table = 'remote_read_connections'
ORDER BY position;

SELECT count() >= 0 FROM system.remote_read_connections;

-- Read data through ReaderExecutor and verify correctness.
DROP TABLE IF EXISTS test_remote_read_conn;
CREATE TABLE test_remote_read_conn (x UInt64) ENGINE = MergeTree ORDER BY x;
SET use_reader_executor = 0;
INSERT INTO test_remote_read_conn SELECT number FROM numbers(10000);

SELECT sum(x) FROM test_remote_read_conn SETTINGS use_reader_executor = 1;

DROP TABLE test_remote_read_conn;
