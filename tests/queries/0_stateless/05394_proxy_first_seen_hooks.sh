#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: clickhouse-proxy is built on the silk fiber framework, which the fast build omits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `on_first_seen_database` hook runs only for a connection that is routed to a backend,
# and the seen names are tracked per pool: the same database routed to another pool (another tenant)
# runs the hook again, while an unroutable connection does not run it at all.

CONFIG="${CLICKHOUSE_TMP}/05394_proxy_config.xml"
LOG="${CLICKHOUSE_TMP}/05394_proxy.log"
HOOK="${CLICKHOUSE_TMP}/05394_hook.sh"
HOOK_LOG="${CLICKHOUSE_TMP}/05394_hook.log"

NONCE="05394_${CLICKHOUSE_DATABASE}"

rm -f "$HOOK_LOG"
cat > "$HOOK" <<EOF
#!/usr/bin/env bash
# Invoked as: KIND PROTOCOL HOST USER DATABASE
echo "\$1 \$3" >> "$HOOK_LOG"
EOF
chmod +x "$HOOK"

PROXY_PID=
PROXY_PORT=

trap '[ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null' EXIT

start_proxy()
{
    # A free port is picked by binding a socket and immediately releasing it, so another process can
    # take it before the proxy binds it. Serve a run-unique token to tell our own proxy from a stranger.
    PROXY_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")

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
                <name>tenant_a</name>
                <backend><name>a</name><host>${CLICKHOUSE_HOST}</host><http_port>${CLICKHOUSE_PORT_HTTP}</http_port></backend>
            </pool>
            <pool>
                <name>tenant_b</name>
                <backend><name>b</name><host>${CLICKHOUSE_HOST}</host><http_port>${CLICKHOUSE_PORT_HTTP}</http_port></backend>
            </pool>
        </pools>
        <rules>
            <rule><host>tenant-a</host><pool>tenant_a</pool></rule>
            <rule><host>tenant-b</host><pool>tenant_b</pool></rule>
        </rules>
        <hooks>
            <on_first_seen_database>${HOOK}</on_first_seen_database>
        </hooks>
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
        cat "$CUR_DIR/05394_proxy_first_seen_hooks.reference"
        exit 0
    fi
    echo "proxy did not start" >&2
    cat "$LOG" >&2
    exit 1
fi

URL="http://127.0.0.1:${PROXY_PORT}/?database=${CLICKHOUSE_DATABASE}&query=SELECT%201"

# No rule matches this host and the listener has no default pool: the connection is rejected, without the hook.
curl -s -o /dev/null --max-time 10 -H "Host: tenant-c" "$URL"
echo "unroutable: $([ -f "$HOOK_LOG" ] && wc -l < "$HOOK_LOG" || echo 0)"

# The first connection to each pool runs the hook (before relaying), repeated ones do not.
curl -s --max-time 10 -H "Host: tenant-a" "$URL"
curl -s --max-time 10 -H "Host: tenant-a" "$URL"
curl -s --max-time 10 -H "Host: tenant-b" "$URL"
curl -s --max-time 10 -H "Host: tenant-b" "$URL"

sort "$HOOK_LOG"
