DROP TABLE IF EXISTS test;
CREATE TABLE test (x String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test VALUES ('Hello, world');

SET min_bytes_to_use_mmap_io = 1;
SELECT * FROM test;

SYSTEM FLUSH LOGS;
SELECT PE.Values FROM system.query_log ARRAY JOIN ProfileEvents AS PE WHERE event_date >= yesterday() AND event_time >= now() - 300 AND query LIKE 'SELECT * FROM test%' AND PE.Names = 'CreatedReadBufferMMap' AND type = 2 ORDER BY event_time DESC LIMIT 1;

DROP TABLE test;
