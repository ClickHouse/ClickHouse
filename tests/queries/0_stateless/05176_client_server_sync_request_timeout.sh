#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_NAME="user_05176_${CLICKHOUSE_DATABASE}"
PROXY_PORT_FILE="${CLICKHOUSE_TMP}/05176_proxy_${CLICKHOUSE_DATABASE}.port"
CLIENT_CONFIG="${CLICKHOUSE_TMP}/05176_client_${CLICKHOUSE_DATABASE}.xml"

rm -f "$PROXY_PORT_FILE"
printf '<clickhouse/>\n' > "$CLIENT_CONFIG"

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP USER IF EXISTS ${USER_NAME};
    CREATE USER ${USER_NAME} SETTINGS sync_request_timeout = 10, apply_settings_from_server = 1;
    GRANT CREATE TEMPORARY TABLE, SELECT ON *.* TO ${USER_NAME};
    GRANT TABLE ENGINE ON Memory TO ${USER_NAME};
"

python3 "$CUR_DIR"/helpers/delaying_tcp_proxy.py "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT_TCP" 6 "$PROXY_PORT_FILE" &
PROXY_PID=$!

cleanup()
{
    kill "$PROXY_PID" 2>/dev/null
    rm -f "$PROXY_PORT_FILE" "$CLIENT_CONFIG"
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USER_NAME}" >/dev/null 2>&1
}
trap cleanup EXIT

for _ in {1..300}; do
    [ -s "$PROXY_PORT_FILE" ] && break
    sleep 0.1
done

PROXY_PORT=$(cat "$PROXY_PORT_FILE")

${CLICKHOUSE_CLIENT_BINARY} --config-file "$CLIENT_CONFIG" --host 127.0.0.1 --port "$PROXY_PORT" --database "$CLICKHOUSE_DATABASE" --user "$USER_NAME" --multiquery "
    CREATE TEMPORARY TABLE t_05176 ENGINE = Memory AS SELECT 1 AS x;
    SELECT throwIf(1) SETTINGS sync_request_timeout = 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
    SELECT 'the server timeout kept the session' FROM t_05176;
"
