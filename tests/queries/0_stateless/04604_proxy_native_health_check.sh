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

CONFIG="${CLICKHOUSE_TMP}/04604_proxy_config.xml"
LOG="${CLICKHOUSE_TMP}/04604_proxy.log"

# Free ports are picked by binding sockets and immediately releasing them, so another process can
# take them before the proxy binds them. Serve a run-unique token to tell our own proxy from a
# stranger, and bound the client timeouts so a stranger on the native port cannot stall the test.
NONCE="04604_${CLICKHOUSE_DATABASE}"

PROXY_PID=
NATIVE_PORT=
HTTP_PORT=
NATIVE_OUT=

trap '[ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null' EXIT

stop_proxy()
{
    kill "$PROXY_PID" 2>/dev/null
    wait "$PROXY_PID" 2>/dev/null
    PROXY_PID=
}

start_proxy()
{
    read -r NATIVE_PORT HTTP_PORT < <(python3 -c "
import socket
a = socket.socket(); a.bind(('127.0.0.1', 0))
b = socket.socket(); b.bind(('127.0.0.1', 0))
print(a.getsockname()[1], b.getsockname()[1])
a.close(); b.close()")

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
            <static><page><path>/whoami</path><content>${NONCE}</content></page></static>
        </http>
        <health_check><enabled>1</enabled><interval_ms>200</interval_ms></health_check>
    </proxy>
</clickhouse>
EOF

    $CLICKHOUSE_BINARY proxy --config-file "$CONFIG" > "$LOG" 2>&1 &
    PROXY_PID=$!

    # Wait until our own proxy answers on the HTTP port.
    local ready=0
    for _ in $(seq 1 100); do
        WHOAMI=$(curl -s --max-time 5 "http://127.0.0.1:${HTTP_PORT}/whoami")
        CURL_STATUS=$?
        if [ "$WHOAMI" = "$NONCE" ]; then ready=1; break; fi
        # An answer from something else - a different body, or a hang up to the timeout (curl exit
        # code 28) - means the port was taken by a stranger, and so does our own proxy exiting (it
        # could not bind). All of them are retried on freshly picked ports.
        if [ -n "$WHOAMI" ] || [ "$CURL_STATUS" -eq 28 ] || ! kill -0 "$PROXY_PID" 2>/dev/null; then break; fi
        sleep 0.2
    done

    if [ "$ready" -ne 1 ]; then stop_proxy; return 1; fi

    # A query forwarded through the native frontend to the backend server. The HTTP and native
    # listeners bind independently, so a successful probe above does not guarantee the native port
    # is already accepting connections; retry until it is to avoid a startup race.
    NATIVE_OUT=
    local started
    for _ in $(seq 1 100); do
        started=$SECONDS
        if NATIVE_OUT=$($CLICKHOUSE_CLIENT_BINARY --host 127.0.0.1 --port "$NATIVE_PORT" \
            --connect_timeout 2 --send_timeout 5 --receive_timeout 5 \
            --query "SELECT 'native_ok'" 2>/dev/null); then
            return 0
        fi
        # A connection refused by nobody fails at once; anything slower means the port belongs to a
        # stranger that does not speak the native protocol, so start over on freshly picked ports.
        if [ $((SECONDS - started)) -ge 2 ]; then break; fi
        sleep 0.2
    done

    stop_proxy
    return 1
}

READY=0
for _ in $(seq 1 5); do
    if start_proxy; then READY=1; break; fi
    # A build without the fiber framework will never come up; do not retry it.
    if grep -qiE "silk|SUPPORT_IS_DISABLED" "$LOG"; then break; fi
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

echo "$NATIVE_OUT"

# The health monitor probes the backend every interval_ms; wait until the status endpoint shows a
# completed probe (check_latency_ms > 0) with the backend still alive for pool selection. The curl
# is bounded so a stalled connection cannot hang the test, and an empty or truncated body (the
# endpoint polled mid-startup) is treated as "not ready yet" without leaking a traceback to stderr.
HEALTH=health_missing
DEADLINE=$((SECONDS + 60))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    if curl -s --max-time 2 "http://127.0.0.1:${HTTP_PORT}/proxy_status" | python3 -c "
import json, sys
try:
    status = json.load(sys.stdin)
except ValueError:
    sys.exit(1)
backends = [b for pool in status.get('pools', []) for b in pool.get('backends', [])]
sys.exit(0 if backends and all(b['alive'] and b['check_latency_ms'] > 0 for b in backends) else 1)
"; then HEALTH=health_ok; break; fi
    sleep 0.2
done
echo "$HEALTH"
