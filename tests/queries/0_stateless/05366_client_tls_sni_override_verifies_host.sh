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
</clickhouse>
EOF

function client()
{
    ${CLICKHOUSE_CLIENT_BINARY} --config-file="$DIR/client.xml" --port "${CLICKHOUSE_PORT_TCP_SECURE}" --secure "$@" -q "SELECT 1" 2>&1
}

client --host localhost
client --host localhost --tls-sni-override a.b.example.com
client --host 127.0.0.1 --tls-sni-override a.b.example.com | grep -o -m1 'Unacceptable certificate'
