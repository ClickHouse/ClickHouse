DROP TABLE IF EXISTS test_01343;
CREATE TABLE test_01343 (x String) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO test_01343 VALUES ('Hello, world');

SET min_bytes_to_use_mmap_io = 1;
SELECT * FROM test_01343;

SYSTEM FLUSH LOGS;
SELECT PE.Values FROM system.query_log ARRAY JOIN ProfileEvents AS PE WHERE event_date >= yesterday() AND event_time >= now() - 300 AND query LIKE 'SELECT * FROM test_01343%' AND PE.Names = 'CreatedReadBufferMMap' AND type = 2 ORDER BY event_time DESC LIMIT 1;

DROP TABLE test_01343;
