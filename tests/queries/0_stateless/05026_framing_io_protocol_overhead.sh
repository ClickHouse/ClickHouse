#!/usr/bin/env bash
# Tags: no-parallel-replicas
# Tag no-parallel-replicas: replica connections add network traffic that is not this response's framing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `FramingServiceBytes` covers HTTP service frames; HTTP queries must not emit `NativeProtocolDataBytes`.

query_id_prefix="05026_framing_io_protocol_overhead_${CLICKHOUSE_DATABASE}"
# `http_response_buffer_size` / `http_wait_end_of_query` (both randomized) put a `CascadeWriteBuffer`
# between the framing and the socket, which is exactly the case the counter stays zero for - pin them off.
framing_url="${CLICKHOUSE_URL}&framing_output_format=EventStream&send_logs_level=trace"
framing_url="${framing_url}&http_response_buffer_size=0&http_wait_end_of_query=0"

# `FORMAT Null` sends no data to the client, so every byte the framing writes during these two
# seconds belongs to a service packet.
${CLICKHOUSE_CURL} -sS "${framing_url}&query_id=${query_id_prefix}_plain" \
    -d "SELECT sleepEachRow(0.4) FROM numbers(5) FORMAT Null" > /dev/null

# With response compression (the Play UI always asks for it) the framing writes into the gzip wrapper,
# so what it could count is the uncompressed input, not the bytes `NetworkSendBytes` sees - the
# counter must stay zero rather than let the meter subtract an uncompressed figure from compressed
# traffic. The `Content-Encoding` header pins that the response really was compressed.
${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip' -o /dev/null -D - \
    "${framing_url}&enable_http_compression=1&query_id=${query_id_prefix}_gzip" \
    -d "SELECT sleepEachRow(0.4) FROM numbers(5) FORMAT Null" | grep -i '^Content-Encoding:' | tr -d '\r'

# A compressed framed query that does stream result bytes: the response carries real data bytes and
# still counts none of them - nor of its service packets - as `FramingServiceBytes`.
${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip' \
    "${framing_url}&enable_http_compression=1&query_id=${query_id_prefix}_gzip_data" \
    -d "SELECT number FROM numbers(1000000) FORMAT TSV" > /dev/null

# The query_log entry is written after the HTTP response is sent, so wait for it.
for _ in {1..60}; do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    count=$(${CLICKHOUSE_CLIENT} -q "
SELECT count()
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND query_id LIKE '${query_id_prefix}%'")
    [ "$count" -ge 3 ] && break
    sleep 0.5
done

${CLICKHOUSE_CLIENT} -q "
SELECT
    'framed idle query uses no native data packets',
    ProfileEvents['FramingServiceBytes'] > 0,
    ProfileEvents['NativeProtocolDataBytes'] = 0,
    2 * ProfileEvents['FramingServiceBytes'] >= ProfileEvents['NetworkSendBytes']
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND query_id = '${query_id_prefix}_plain'"

${CLICKHOUSE_CLIENT} -q "
SELECT
    'compressed framed idle query uses no native data packets',
    ProfileEvents['FramingServiceBytes'] = 0,
    ProfileEvents['NativeProtocolDataBytes'] = 0,
    ProfileEvents['NetworkSendBytes'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND query_id = '${query_id_prefix}_gzip'"

${CLICKHOUSE_CLIENT} -q "
SELECT
    'compressed framed result uses no native data packets',
    ProfileEvents['FramingServiceBytes'] = 0,
    ProfileEvents['NativeProtocolDataBytes'] = 0,
    ProfileEvents['NetworkSendBytes'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND query_id = '${query_id_prefix}_gzip_data'"
