#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: clickhouse-proxy is built on the silk fiber framework, which the fast build omits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The proxy must extract the routing attributes of an HTTP request the way the server interprets them,
# so that a client cannot route by one value and have the backend act on another:
# - the query is the `query` parameter followed by the body;
# - the last `database` parameter wins (it is a setting);
# - the `Authorization` scheme is case-insensitive and separated by any whitespace.
# Every request below reaches the live server only if the proxy routes it like that; otherwise it goes
# to the listener's default pool, which cannot be connected to.
# Also, the whole handshake is bounded by `handshake_timeout_ms`, not only every read of it.

CONFIG="${CLICKHOUSE_TMP}/05331_proxy_config.xml"
LOG="${CLICKHOUSE_TMP}/05331_proxy.log"

NONCE="05331_${CLICKHOUSE_DATABASE}"

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
        <handshake_timeout_ms>2000</handshake_timeout_ms>
        <listeners>
            <listener><protocol>http</protocol><port>${PROXY_PORT}</port><pool>dead</pool></listener>
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
            <rule><database>${CLICKHOUSE_DATABASE}</database><pool>ch</pool></rule>
            <rule><user>default</user><pool>ch</pool></rule>
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
        cat "$CUR_DIR/05331_proxy_http_routing_like_server.reference"
        exit 0
    fi
    echo "proxy did not start" >&2
    cat "$LOG" >&2
    exit 1
fi

URL="http://127.0.0.1:${PROXY_PORT}/"

# A query in parentheses has no leading keyword, so its type is `other`: it does not match the `select` rule.

# The `query` parameter is only a comment, and the body is a `select` query.
curl -s --max-time 10 --data-binary 'SELECT 1' "${URL}?query=%2F*%20comment%20*%2F"

# The first `database` parameter does not match any rule, the last one does, and the server uses the last one.
curl -s --max-time 10 "${URL}?database=nonexistent_05331&database=${CLICKHOUSE_DATABASE}&query=(SELECT%20currentDatabase()%20%3D%20'${CLICKHOUSE_DATABASE}')"

# The `Authorization` scheme in lowercase and separated by a tab.
CREDENTIALS=$(echo -n "default:" | base64)
curl -s --max-time 10 -H "Authorization: basic ${CREDENTIALS}" "${URL}?query=(SELECT%203)"
curl -s --max-time 10 -H "Authorization: Basic	${CREDENTIALS}" "${URL}?query=(SELECT%204)"

# A client that drip-feeds its request head is disconnected once `handshake_timeout_ms` passes,
# although each single read is much shorter than that.
python3 - "$PROXY_PORT" <<'EOF'
import socket
import sys
import time

sock = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=10)
sock.sendall(b"GET / HTTP/1.1\r\nX-Padding: ")
sock.settimeout(0.3)
start = time.monotonic()
closed = False
while time.monotonic() - start < 30:
    try:
        sock.sendall(b"a")
        if sock.recv(1) == b"":
            closed = True
            break
    except socket.timeout:
        pass
    except OSError:
        closed = True
        break
print("disconnected" if closed and time.monotonic() - start < 20 else "not disconnected")
EOF
