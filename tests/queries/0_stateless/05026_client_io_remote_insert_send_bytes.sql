-- Tags: no-parallel-replicas
-- no-parallel-replicas: replica connections add coordination packets of their own.

-- The initiator of a distributed `INSERT` sends the blocks to the remote server. They count to
-- `NativeProtocolDataBytes`, so the client's IO meter shows the upload while it runs.

DROP TABLE IF EXISTS t_05026_remote_insert;
CREATE TABLE t_05026_remote_insert (s String) ENGINE = MergeTree ORDER BY tuple();

SET log_queries = 1;

-- About 1 MB of random strings, which do not compress.
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), t_05026_remote_insert)
SELECT randomString(1000) FROM numbers(1000)
SETTINGS prefer_localhost_replica = 0, log_comment = '05026_client_io_remote_insert_send_bytes';

SYSTEM FLUSH LOGS query_log;

SELECT
    'the sent blocks are counted',
    ProfileEvents['NativeProtocolDataBytes'] > 1000000,
    'not much more than the data',
    ProfileEvents['NativeProtocolDataBytes'] < 1100000
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
    AND log_comment = '05026_client_io_remote_insert_send_bytes';

SELECT count() FROM t_05026_remote_insert;

DROP TABLE t_05026_remote_insert;
