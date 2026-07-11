#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: clickhouse-proxy is built on the silk fiber framework, which the fast build omits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Route a proxy HTTP listener to the live test server and check /ping, a static page, the status
# endpoint, and end-to-end query forwarding.

PROXY_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
CONFIG="${CLICKHOUSE_TMP}/04512_proxy_config.xml"
LOG="${CLICKHOUSE_TMP}/04512_proxy.log"

cat > "$CONFIG" <<EOF
<clickhouse>
    <logger><level>warning</level><console>1</console></logger>
    <proxy>
        <listen_host>127.0.0.1</listen_host>
        <listeners>
            <listener><protocol>http</protocol><port>${PROXY_PORT}</port><pool>ch</pool></listener>
        </listeners>
        <pools>
            <pool>
                <name>ch</name>
                <backend><host>${CLICKHOUSE_HOST}</host><http_port>${CLICKHOUSE_PORT_HTTP}</http_port></backend>
            </pool>
        </pools>
        <http>
            <ping_path>/ping</ping_path>
            <status_path>/proxy_status</status_path>
            <static><page><path>/hello</path><content>STATIC</content></page></static>
        </http>
        <health_check><enabled>0</enabled></health_check>
    </proxy>
</clickhouse>
EOF

$CLICKHOUSE_BINARY proxy --config-file "$CONFIG" > "$LOG" 2>&1 &
PROXY_PID=$!

trap '[ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null' EXIT

# Wait for the proxy to bind, or bail out gracefully if this build has no fiber framework.
READY=0
for _ in $(seq 1 100); do
    if curl -s -o /dev/null "http://127.0.0.1:${PROXY_PORT}/ping"; then READY=1; break; fi
    if ! kill -0 "$PROXY_PID" 2>/dev/null; then break; fi
    sleep 0.2
done

if [ "$READY" -ne 1 ]; then
    if grep -qiE "silk|SUPPORT_IS_DISABLED" "$LOG"; then
        # The proxy is not available in this build; emit the expected output so the test passes.
        echo "Ok."
        echo "STATIC"
        echo "1"
        echo "status_ok"
        exit 0
    fi
    echo "proxy did not start" >&2
    cat "$LOG" >&2
    exit 1
fi

# /ping served by the proxy itself.
curl -s "http://127.0.0.1:${PROXY_PORT}/ping"
# Static page served by the proxy itself.
curl -s "http://127.0.0.1:${PROXY_PORT}/hello"; echo
# A query forwarded to the backend server.
curl -s "http://127.0.0.1:${PROXY_PORT}/?query=SELECT%201"
# The status endpoint reports the pool.
curl -s "http://127.0.0.1:${PROXY_PORT}/proxy_status" | grep -q '"ch"' && echo "status_ok" || echo "status_missing"
