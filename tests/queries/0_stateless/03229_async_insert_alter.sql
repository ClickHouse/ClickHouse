-- Tags: no-parallel
-- no-parallel because the test uses FLUSH ASYNC INSERT QUEUE

SET wait_for_async_insert = 0;
SET async_insert_busy_timeout_max_ms = 300000;
SET async_insert_busy_timeout_min_ms = 300000;
SET async_insert_use_adaptive_busy_timeout = 0;

DROP TABLE IF EXISTS t_async_insert_alter;

CREATE TABLE t_async_insert_alter (id Int64, v1 Int64) ENGINE = MergeTree ORDER BY id SETTINGS async_insert = 1;

-- ADD COLUMN

INSERT INTO t_async_insert_alter VALUES (42, 24);

ALTER TABLE t_async_insert_alter ADD COLUMN value2 Int64;

SYSTEM FLUSH ASYNC INSERT QUEUE;
SYSTEM FLUSH LOGS;

SELECT * FROM t_async_insert_alter ORDER BY id;

-- MODIFY COLUMN

INSERT INTO t_async_insert_alter VALUES (43, 34, 55);

ALTER TABLE t_async_insert_alter MODIFY COLUMN value2 String;

SYSTEM FLUSH ASYNC INSERT QUEUE;
SYSTEM FLUSH LOGS;

SELECT * FROM t_async_insert_alter ORDER BY id;

-- DROP COLUMN

INSERT INTO t_async_insert_alter VALUES ('100', '200', '300');

ALTER TABLE t_async_insert_alter DROP COLUMN value2;

SYSTEM FLUSH ASYNC INSERT QUEUE;
SYSTEM FLUSH LOGS;

SELECT * FROM t_async_insert_alter ORDER BY id;
SELECT query, data_kind, status FROM system.asynchronous_insert_log WHERE database = currentDatabase() AND table = 't_async_insert_alter' ORDER BY event_time_microseconds;

DROP TABLE t_async_insert_alter;
