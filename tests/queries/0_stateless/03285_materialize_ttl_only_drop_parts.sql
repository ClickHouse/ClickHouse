DROP TABLE IF EXISTS test_03285_mat_ttl;

CREATE TABLE test_03285_mat_ttl (id UInt64, value String, event_time DateTime)
    ENGINE MergeTree()
        ORDER BY id
        SETTINGS min_bytes_for_wide_part = 1000000000, index_granularity = 8192, index_granularity_bytes = '10Mi', ttl_only_drop_parts=1;

INSERT INTO test_03285_mat_ttl SELECT number, randomString(10), now() + INTERVAL 2 MONTH FROM numbers(50000);
INSERT INTO test_03285_mat_ttl SELECT number, randomString(10), now() FROM numbers(50000);

OPTIMIZE TABLE test_03285_mat_ttl FINAL SETTINGS mutations_sync = 1;

SET mutations_sync=1;

SELECT 'Rows in parts', SUM(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'test_03285_mat_ttl' AND active;
SELECT 'Count', count() FROM test_03285_mat_ttl;

ALTER TABLE test_03285_mat_ttl MODIFY TTL event_time + INTERVAL 1 MONTH SETTINGS mutations_sync = 1;
OPTIMIZE TABLE test_03285_mat_ttl FINAL SETTINGS mutations_sync = 1;

SELECT 'Rows in parts', SUM(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'test_03285_mat_ttl' AND active;
SELECT 'Count', count() FROM test_03285_mat_ttl;

ALTER TABLE test_03285_mat_ttl MODIFY TTL event_time - INTERVAL 3 MONTH SETTINGS mutations_sync = 1;
OPTIMIZE TABLE test_03285_mat_ttl FINAL SETTINGS mutations_sync = 1;

SELECT 'Rows in parts', SUM(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'test_03285_mat_ttl' AND active;
SELECT 'Count', count() FROM test_03285_mat_ttl;

DROP TABLE test_03285_mat_ttl;
