#!/usr/bin/env bash
# Tags: no-parallel

CLICKHOUSE_PORT_TCP=50111
CLICKHOUSE_DATABASE=default

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "Starting clickhouse-server"

$CLICKHOUSE_BINARY server -- --tcp_port "$CLICKHOUSE_PORT_TCP" --path "${CLICKHOUSE_TMP}/" > "${CLICKHOUSE_TMP}/server.log" 2>&1 &
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
