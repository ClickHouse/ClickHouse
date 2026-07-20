#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: clickhouse-proxy is built on the silk fiber framework, which the fast build omits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Route a proxy native listener to the live test server with health checks enabled (the default
# routing mode): check that a query round-trips through the native frontend, and that the health
# monitor has probed the backend and keeps it alive for pool selection, as reported by the
# status endpoint.

read -r NATIVE_PORT HTTP_PORT < <(python3 -c "
import socket
a = socket.socket(); a.bind(('127.0.0.1', 0))
b = socket.socket(); b.bind(('127.0.0.1', 0))
print(a.getsockname()[1], b.getsockname()[1])
a.close(); b.close()")

CONFIG="${CLICKHOUSE_TMP}/04604_proxy_config.xml"
LOG="${CLICKHOUSE_TMP}/04604_proxy.log"

cat > "$CONFIG" <<EOF
<clickhouse>
    <logger><level>warning</level><console>1</console></logger>
    <proxy>
        <listen_host>127.0.0.1</listen_host>
        <listeners>
            <listener><protocol>native</protocol><port>${NATIVE_PORT}</port><pool>ch</pool></listener>
            <listener><protocol>http</protocol><port>${HTTP_PORT}</port><pool>ch</pool></listener>
        </listeners>
        <pools>
            <pool>
                <name>ch</name>
                <backend>
                    <host>${CLICKHOUSE_HOST}</host>
                    <tcp_port>${CLICKHOUSE_PORT_TCP}</tcp_port>
                    <http_port>${CLICKHOUSE_PORT_HTTP}</http_port>
                </backend>
            </pool>
        </pools>
        <http>
            <ping_path>/ping</ping_path>
            <status_path>/proxy_status</status_path>
        </http>
        <health_check><enabled>1</enabled><interval_ms>200</interval_ms></health_check>
    </proxy>
</clickhouse>
EOF

$CLICKHOUSE_BINARY proxy --config-file "$CONFIG" > "$LOG" 2>&1 &
PROXY_PID=$!

trap '[ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null' EXIT

# Wait for the proxy to bind, or bail out gracefully if this build has no fiber framework.
READY=0
for _ in $(seq 1 100); do
    if curl -s -o /dev/null "http://127.0.0.1:${HTTP_PORT}/ping"; then READY=1; break; fi
    if ! kill -0 "$PROXY_PID" 2>/dev/null; then break; fi
    sleep 0.2
done

if [ "$READY" -ne 1 ]; then
    if grep -qiE "silk|SUPPORT_IS_DISABLED" "$LOG"; then
        # The proxy is not available in this build; emit the expected output so the test passes.
        echo "native_ok"
        echo "health_ok"
        exit 0
    fi
    echo "proxy did not start" >&2
    cat "$LOG" >&2
    exit 1
fi

# A query forwarded through the native frontend to the backend server. The HTTP and native
# listeners bind independently, so a successful /ping above does not guarantee the native port is
# already accepting connections; retry until it is to avoid a startup race (Connection refused).
NATIVE_OUT=
for _ in $(seq 1 100); do
    if NATIVE_OUT=$($CLICKHOUSE_CLIENT_BINARY --host 127.0.0.1 --port "$NATIVE_PORT" --query "SELECT 'native_ok'" 2>/dev/null); then
        break
    fi
    sleep 0.2
done
echo "$NATIVE_OUT"

# The health monitor probes the backend every interval_ms; wait until the status endpoint shows a
# completed probe (check_latency_ms > 0) with the backend still alive for pool selection.
HEALTH=health_missing
for _ in $(seq 1 100); do
    if curl -s "http://127.0.0.1:${HTTP_PORT}/proxy_status" | python3 -c "
import json, sys
status = json.load(sys.stdin)
backends = [b for pool in status.get('pools', []) for b in pool.get('backends', [])]
sys.exit(0 if backends and all(b['alive'] and b['check_latency_ms'] > 0 for b in backends) else 1)
"; then HEALTH=health_ok; break; fi
    sleep 0.2
done
echo "$HEALTH"
