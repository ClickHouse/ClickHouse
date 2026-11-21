-- Tags: no-parallel-replicas
-- no-parallel-replicas: replica connections add coordination packets of their own.

-- The initiator receives the remote server's `Progress` and `ProfileEvents` packets, which are not query data.
-- They count to `NetworkReceiveBytes`, but not to `NativeProtocolDataReceiveBytes`, which the client's IO meter uses.

SET log_queries = 1;

-- One row of data comes back, while the remote server reports progress for about a second.
SELECT count() FROM remote('127.0.0.1', numbers(20)) WHERE sleepEachRow(0.05) = 0
SETTINGS prefer_localhost_replica = 0, max_block_size = 1, log_comment = '05026_client_io_remote_data_receive_bytes'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    'the result block is counted',
    ProfileEvents['NativeProtocolDataReceiveBytes'] > 0,
    'it is small',
    ProfileEvents['NativeProtocolDataReceiveBytes'] < 1000,
    'the remote service packets are not counted',
    ProfileEvents['NetworkReceiveBytes'] > ProfileEvents['NativeProtocolDataReceiveBytes'] + 1000
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
    AND log_comment = '05026_client_io_remote_data_receive_bytes';
