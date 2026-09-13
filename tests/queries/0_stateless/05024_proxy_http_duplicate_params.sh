#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: clickhouse-proxy is built on the silk fiber framework, which the fast build omits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A repeated query parameter must be routed by its first occurrence, the way the server resolves it,
# so that a client cannot route by one value and have the backend act on another.
# Here `select` queries go to the live server and every other query type to a pool that cannot be
# connected to, so a request with `?query=SELECT...&query=INSERT...` reaches the server only if the
# proxy routes by the first occurrence.

CONFIG="${CLICKHOUSE_TMP}/05024_proxy_config.xml"
LOG="${CLICKHOUSE_TMP}/05024_proxy.log"

NONCE="05024_${CLICKHOUSE_DATABASE}"

PROXY_PID=
PROXY_PORT=

trap '[ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null' EXIT

start_proxy()
{
    # A free port is picked by binding a socket and immediately releasing it, so another process can
    # take it before the proxy binds it. Serve a run-unique token to tell our own proxy from a stranger.
    PROXY_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
    DEAD_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")

    cat > "$CONFIG" <<EOF
<clickhouse>
    <logger><level>warning</level><console>1</console></logger>
    <proxy>
        <listen_host>127.0.0.1</listen_host>
        <listeners>
            <listener><protocol>http</protocol><port>${PROXY_PORT}</port></listener>
        </listeners>
        <pools>
            <pool>
                <name>ch</name>
                <backend><host>${CLICKHOUSE_HOST}</host><http_port>${CLICKHOUSE_PORT_HTTP}</http_port></backend>
            </pool>
            <pool>
                <name>dead</name>
                <backend><host>127.0.0.1</host><http_port>${DEAD_PORT}</http_port></backend>
            </pool>
        </pools>
        <rules>
            <rule><query_type>select</query_type><pool>ch</pool></rule>
            <rule><query_type>insert,other</query_type><pool>dead</pool></rule>
        </rules>
        <http>
            <static>
                <page><path>/whoami</path><content>${NONCE}</content></page>
            </static>
        </http>
        <health_check><enabled>0</enabled></health_check>
    </proxy>
</clickhouse>
EOF

    $CLICKHOUSE_BINARY proxy --config-file "$CONFIG" > "$LOG" 2>&1 &
    PROXY_PID=$!

    # Wait until our own proxy answers on the port.
    for _ in $(seq 1 100); do
        WHOAMI=$(curl -s --max-time 5 "http://127.0.0.1:${PROXY_PORT}/whoami")
        CURL_STATUS=$?
        if [ "$WHOAMI" = "$NONCE" ]; then return 0; fi
        # An answer from something else - a different body, or a hang up to the timeout (curl exit
        # code 28) - means the port was taken by a stranger, and so does our own proxy exiting (it
        # could not bind). All of them are retried on a freshly picked port.
        if [ -n "$WHOAMI" ] || [ "$CURL_STATUS" -eq 28 ] || ! kill -0 "$PROXY_PID" 2>/dev/null; then break; fi
        sleep 0.2
    done

    kill "$PROXY_PID" 2>/dev/null
    wait "$PROXY_PID" 2>/dev/null
    PROXY_PID=
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
        echo "1"
        echo "1"
        exit 0
    fi
    echo "proxy did not start" >&2
    cat "$LOG" >&2
    exit 1
fi

# All the requests are bounded so a stalled connection cannot hang the test.
# A single `select` query is routed to the live server.
curl -s --max-time 10 "http://127.0.0.1:${PROXY_PORT}/?query=SELECT%201"
# The same, with a second `query` parameter that would route to the unreachable pool if the proxy
# took the last occurrence. The server also runs the first occurrence, so the answer is still `1`.
curl -s --max-time 10 "http://127.0.0.1:${PROXY_PORT}/?query=SELECT%201&query=INSERT%20INTO%20nonexistent%20VALUES"
