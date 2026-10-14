#!/usr/bin/env bash

CLICKHOUSE_PORT_TCP=50121
CLICKHOUSE_DATABASE=default

# As in `01507_clickhouse_server_start_with_embedded_config`: the embedded default config opens the
# HTTP, MySQL, PostgreSQL and interserver listeners too, and on macOS this second server can bind
# `127.0.0.1:8123` next to the main server's `::` bind. `clickhouse-test` creates every per-test
# database over HTTP, so such a steal makes unrelated tests fail with `UNKNOWN_DATABASE`. Every
# listener gets its own port, from a different block than `01507` so the two tests never collide.
SERVER_PORT_HTTP=50122
SERVER_PORT_MYSQL=50123
SERVER_PORT_POSTGRESQL=50124
SERVER_PORT_INTERSERVER=50125

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "Starting clickhouse-server"

$CLICKHOUSE_BINARY server -- --tcp_port "$CLICKHOUSE_PORT_TCP" --http_port "$SERVER_PORT_HTTP" \
    --mysql_port "$SERVER_PORT_MYSQL" --postgresql_port "$SERVER_PORT_POSTGRESQL" \
    --interserver_http_port "$SERVER_PORT_INTERSERVER" \
    --path "${CLICKHOUSE_TMP}/" > "${CLICKHOUSE_TMP}/server.log" 2>&1 &
PID=$!

echo "Waiting for clickhouse-server to start"

for i in {1..30}; do
    sleep 1
    $CLICKHOUSE_CLIENT --query "SELECT 1" 2>/dev/null && break
    if [[ $i == 30 ]]; then
        cat "${CLICKHOUSE_TMP}/server.log"
        exit 1
    fi
done

# Make sure the directory for the default database is created:
$CLICKHOUSE_CLIENT --query "CREATE TABLE test (x UInt8) ORDER BY ()"

kill $PID
wait

$CLICKHOUSE_LOCAL --path "${CLICKHOUSE_TMP}/" --query "
    SELECT uuid = '$(basename $(readlink ${CLICKHOUSE_TMP}/metadata/default))' FROM system.databases WHERE name = 'default'
" || cat "${CLICKHOUSE_TMP}/server.log"
