-- Tags: no-parallel-replicas
-- no-parallel-replicas: replica connections add network traffic that is not the client protocol's.

-- `NativeProtocolDataBytes` counts result packets without counting the protocol's own progress traffic.

SET log_queries = 1;

SELECT 1 SETTINGS log_comment = '05026_client_io_protocol_overhead result query';

-- `FORMAT Null` sends no result data, while progress packets still produce `NetworkSendBytes`.
SELECT sleepEachRow(0.4) FROM numbers(5) FORMAT Null
SETTINGS log_comment = '05026_client_io_protocol_overhead idle query';

SYSTEM FLUSH LOGS query_log;

SELECT
    'result query sends native data packets',
    ProfileEvents['NativeProtocolDataBytes'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish' AND current_database = currentDatabase()
    AND log_comment = '05026_client_io_protocol_overhead result query';

SELECT
    'idle query sends no native data packets',
    ProfileEvents['NativeProtocolDataBytes'] = 0,
    ProfileEvents['NetworkSendBytes'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish' AND current_database = currentDatabase()
    AND log_comment = '05026_client_io_protocol_overhead idle query';
