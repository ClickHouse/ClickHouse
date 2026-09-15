-- Tags: no-parallel-replicas
-- no-parallel-replicas: replica connections add network traffic that is not the client protocol's.

-- The live IO meter of clickhouse-client (https://github.com/ClickHouse/ClickHouse/issues/116565)
-- sums `NetworkSendBytes`, which also carries the server's own Progress/ProfileEvents/Logs packets
-- - a floor the meter would otherwise generate for itself and show as IO on an idle query.
-- `NativeProtocolServiceBytes` accounts for exactly those packets, so ProgressIndication can
-- subtract them; this pins the counter the subtraction rides on.

SET log_queries = 1;

-- FORMAT Null sends no data to the client, so every byte leaving the connection during these two
-- seconds is a service packet. log_comment pins the query_log lookup below to this statement.
SELECT sleepEachRow(0.4) FROM numbers(5) FORMAT Null
SETTINGS log_comment = '05026_client_io_protocol_overhead idle query';

SYSTEM FLUSH LOGS query_log;

SELECT
    'idle query sends only protocol service packets',
    ProfileEvents['NativeProtocolServiceBytes'] > 0,
    2 * ProfileEvents['NativeProtocolServiceBytes'] >= ProfileEvents['NetworkSendBytes']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish' AND current_database = currentDatabase()
    AND log_comment = '05026_client_io_protocol_overhead idle query';
