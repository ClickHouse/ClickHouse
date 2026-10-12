#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_$$"
mkdir -p "$DIR"
trap 'rm -rf "$DIR"' EXIT

# Trust exactly the self-signed certificate (CN=localhost) the server presents.
openssl s_client -connect "127.0.0.1:${CLICKHOUSE_PORT_TCP_SECURE}" </dev/null 2>/dev/null | openssl x509 > "$DIR/server.crt"

cat > "$DIR/client.xml" <<EOF
<clickhouse>
    <openSSL>
        <client>
            <caConfig>$DIR/server.crt</caConfig>
            <loadDefaultCAFile>false</loadDefaultCAFile>
        </client>
    </openSSL>
    <tcp_port>0</tcp_port>
    <tcp_port_secure>${CLICKHOUSE_PORT_TCP_SECURE}</tcp_port_secure>
</clickhouse>
EOF

function client()
{
    ${CLICKHOUSE_CLIENT_BINARY} --config-file="$DIR/client.xml" --port "${CLICKHOUSE_PORT_TCP_SECURE}" --secure "$@" -q "SELECT 1" 2>&1
}

client --host localhost
client --host localhost --tls-sni-override a.b.example.com
client --host 127.0.0.1 --tls-sni-override a.b.example.com | grep -o -m1 'Unacceptable certificate'

# Without --port and --secure the client probes tcp_port and tcp_port_secure and reuses the socket that
# answered. Port 0 always refuses, so it is the secure one.
function probe()
{
    ${CLICKHOUSE_CLIENT_BINARY} --config-file="$DIR/client.xml" --tls-sni-override a.b.example.com "$@" -q "SELECT 1" 2>&1
}

probe --host localhost --log-level=trace | grep -o -e '^1$' -e 'Reusing the connection'
# Without the server name on the reused socket, the loopback exemption would accept 127.0.0.1.
probe --host 127.0.0.1 | grep -o -m1 'server name a.b.example.com'
