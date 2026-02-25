-- Tags: no-random-settings, no-azure-blob-storage

DROP TABLE IF EXISTS order_by_desc;

SET enable_filesystem_cache=0;
SET read_through_distributed_cache=0;

CREATE TABLE order_by_desc (u UInt32, s String)
ENGINE MergeTree ORDER BY u PARTITION BY u % 100
SETTINGS index_granularity = 1024, index_granularity_bytes = '10Mi';

INSERT INTO order_by_desc SELECT number, repeat('a', 1024) FROM numbers(1024 * 300);
OPTIMIZE TABLE order_by_desc FINAL;

SELECT s FROM order_by_desc ORDER BY u DESC LIMIT 10 FORMAT Null
SETTINGS max_memory_usage = '400M';

SELECT s FROM order_by_desc ORDER BY u LIMIT 10 FORMAT Null
SETTINGS max_memory_usage = '400M';

SYSTEM FLUSH LOGS query_log;

--- 100 granules for reading from main table, 10 granules for lazy reading
SELECT read_rows <= (1024 * 110) FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase()
AND event_date >= yesterday()
AND lower(query) LIKE lower('SELECT s FROM order_by_desc ORDER BY u%');

DROP TABLE IF EXISTS order_by_desc;
