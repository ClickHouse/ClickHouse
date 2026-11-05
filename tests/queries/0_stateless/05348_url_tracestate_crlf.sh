#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The client's OpenTelemetry `tracestate` is forwarded by `url` as an HTTP header, so CR or LF in it must be rejected.
$CLICKHOUSE_CLIENT \
    --opentelemetry-traceparent "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" \
    --opentelemetry-tracestate $'a=b\r\nX-Injected: 1' \
    -q "SELECT * FROM url('http://localhost:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', 'TSV', 'x UInt8')" 2>&1 \
    | grep -o -m1 'HTTP header "tracestate" has invalid character'

$CLICKHOUSE_CLIENT \
    --opentelemetry-traceparent "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" \
    --opentelemetry-tracestate 'a=b' \
    -q "SELECT * FROM url('http://localhost:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', 'TSV', 'x UInt8')"
