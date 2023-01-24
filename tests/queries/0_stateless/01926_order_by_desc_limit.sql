-- Tags: no-random-settings

DROP TABLE IF EXISTS order_by_desc;

SET remote_fs_enable_cache=0;

CREATE TABLE order_by_desc (u UInt32, s String)
ENGINE MergeTree ORDER BY u PARTITION BY u % 100
SETTINGS index_granularity = 1024;

INSERT INTO order_by_desc SELECT number, repeat('a', 1024) FROM numbers(1024 * 300);
OPTIMIZE TABLE order_by_desc FINAL;

SELECT s FROM order_by_desc ORDER BY u DESC LIMIT 10 FORMAT Null
SETTINGS max_memory_usage = '400M';

SELECT s FROM order_by_desc ORDER BY u LIMIT 10 FORMAT Null
SETTINGS max_memory_usage = '400M';

SYSTEM FLUSH LOGS;

SELECT read_rows < 110000 FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase()
AND event_time > now() - INTERVAL 10 SECOND
AND lower(query) LIKE lower('SELECT s FROM order_by_desc ORDER BY u%');
