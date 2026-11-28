#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_05185"
PROXY_PORT_FILE="${CLICKHOUSE_TMP}/05185_proxy_${CLICKHOUSE_DATABASE}.port"

rm -f "$PROXY_PORT_FILE"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}; CREATE TABLE ${TABLE} (x UInt8) ENGINE = Memory; INSERT INTO ${TABLE} VALUES (1)"

python3 "$CUR_DIR"/helpers/delaying_tcp_proxy.py "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT_TCP" 1.25 "$PROXY_PORT_FILE" &
PROXY_PID=$!

cleanup()
{
    kill "$PROXY_PID" 2>/dev/null
    rm -f "$PROXY_PORT_FILE"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}" >/dev/null 2>&1
}
trap cleanup EXIT

for _ in {1..300}; do
    [ -s "$PROXY_PORT_FILE" ] && break
    sleep 0.1
done

PROXY_PORT=$(cat "$PROXY_PORT_FILE")
QUERY="SELECT count() FROM remote('127.0.0.1:${PROXY_PORT}', '${CLICKHOUSE_DATABASE}', '${TABLE}')"

${CLICKHOUSE_CLIENT} --send_logs_level=none --query "$QUERY SETTINGS prefer_localhost_replica = 0, sync_request_timeout = 0.5" 2>&1 | grep -oF 'ALL_CONNECTION_TRIES_FAILED'
${CLICKHOUSE_CLIENT} --send_logs_level=none --query "$QUERY SETTINGS prefer_localhost_replica = 0, sync_request_timeout = 10"
