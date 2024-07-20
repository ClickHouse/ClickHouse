-- Tags: no-parallel

DROP TABLE IF EXISTS t_async_insert_skip_settings SYNC;

CREATE TABLE t_async_insert_skip_settings (id UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/tables/t_async_insert_skip_settings', '1')
ORDER BY id;

SET async_insert = 1;
SET async_insert_deduplicate = 1;
SET wait_for_async_insert = 0;
-- Disable adaptive timeout to prevent immediate push of the first message (if the queue last push was old)
SET async_insert_use_adaptive_busy_timeout=0;
SET async_insert_busy_timeout_max_ms = 1000000;

SET insert_deduplication_token = '1';
SET log_comment = 'async_insert_skip_settings_1';
INSERT INTO t_async_insert_skip_settings VALUES (1);

SET insert_deduplication_token = '2';
SET log_comment = 'async_insert_skip_settings_2';
INSERT INTO t_async_insert_skip_settings VALUES (1);

SET insert_deduplication_token = '1';
SET log_comment = 'async_insert_skip_settings_3';
INSERT INTO t_async_insert_skip_settings VALUES (2);

SET insert_deduplication_token = '3';
SET log_comment = 'async_insert_skip_settings_4';
INSERT INTO t_async_insert_skip_settings VALUES (2);

SYSTEM FLUSH LOGS;

SELECT 'pending to flush', length(entries.bytes) FROM system.asynchronous_inserts
WHERE database = currentDatabase() AND table = 't_async_insert_skip_settings'
ORDER BY first_update;

SYSTEM FLUSH ASYNC INSERT QUEUE;

SELECT * FROM t_async_insert_skip_settings ORDER BY id;

SYSTEM FLUSH LOGS;

SELECT 'flush queries', uniqExact(flush_query_id) FROM system.asynchronous_insert_log
WHERE database = currentDatabase() AND table = 't_async_insert_skip_settings';

DROP TABLE t_async_insert_skip_settings SYNC;
