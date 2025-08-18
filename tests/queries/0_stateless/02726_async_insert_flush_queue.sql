-- Tags: no-parallel

DROP TABLE IF EXISTS t_async_inserts_flush;

CREATE TABLE t_async_inserts_flush (a UInt64) ENGINE = Memory;

SET async_insert = 1;
SET wait_for_async_insert = 0;
-- Disable adaptive timeout to prevent immediate push of the first message (if the queue last push was old)
SET async_insert_use_adaptive_busy_timeout=0;
SET async_insert_busy_timeout_max_ms = 10000000;

INSERT INTO t_async_inserts_flush VALUES (1) (2);

INSERT INTO t_async_inserts_flush FORMAT JSONEachRow {"a": 10} {"a": 20};

INSERT INTO t_async_inserts_flush FORMAT JSONEachRow {"a": "str"};

INSERT INTO t_async_inserts_flush FORMAT JSONEachRow {"a": 100} {"a": 200};

INSERT INTO t_async_inserts_flush VALUES (3) (4) (5);

SELECT sleep(1) FORMAT Null;

SELECT format, length(entries.query_id) FROM system.asynchronous_inserts
WHERE database = currentDatabase() AND table = 't_async_inserts_flush'
ORDER BY format;

SELECT count() FROM t_async_inserts_flush;

SYSTEM FLUSH ASYNC INSERT QUEUE;

SELECT count() FROM system.asynchronous_inserts
WHERE database = currentDatabase() AND table = 't_async_inserts_flush';

SELECT count() FROM t_async_inserts_flush;

DROP TABLE t_async_inserts_flush;
