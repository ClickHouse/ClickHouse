#!/usr/bin/env bash
# Tags: no-parallel-replicas
# Tag no-parallel-replicas: replica connections add network traffic that is not this response's framing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Play UI mirrors the live IO meter of clickhouse-client: it sums `NetworkSendBytes`, which over
# HTTP also carries the framed `progress` / `log` / `profile_events` packets the meter itself needs -
# a floor the meter would otherwise generate for itself and show as IO on an idle query.
# `NativeProtocolServiceBytes` accounts for those packets on the native protocol only, so framing
# needs its own counter; this pins the one `play.html` subtracts.

query_id="05026_framing_io_protocol_overhead_${CLICKHOUSE_DATABASE}"

# `FORMAT Null` sends no data to the client, so every byte the framing writes during these two
# seconds belongs to a service packet.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&framing_output_format=EventStream&send_logs_level=trace&query_id=${query_id}" \
    -d "SELECT sleepEachRow(0.4) FROM numbers(5) FORMAT Null" > /dev/null

# The query_log entry is written after the HTTP response is sent, so wait for it.
for _ in {1..60}; do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    result=$(${CLICKHOUSE_CLIENT} -q "
SELECT
    'framed idle query sends only service packets',
    ProfileEvents['FramingServiceBytes'] > 0,
    ProfileEvents['NativeProtocolServiceBytes'] = 0,
    2 * ProfileEvents['FramingServiceBytes'] >= ProfileEvents['NetworkSendBytes']
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND query_id = '${query_id}'")
    [ -n "$result" ] && break
    sleep 0.5
done

echo "$result"
