#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PROXY_PORT_FILE="${CLICKHOUSE_TMP}/05182_proxy_${CLICKHOUSE_DATABASE}.port"
CLIENT_CONFIG="${CLICKHOUSE_TMP}/05182_client_${CLICKHOUSE_DATABASE}.xml"

rm -f "$PROXY_PORT_FILE"

python3 "$CUR_DIR"/helpers/delaying_tcp_proxy.py "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT_TCP" 1.25 "$PROXY_PORT_FILE" &
PROXY_PID=$!
# shellcheck disable=SC2064
trap "kill $PROXY_PID 2>/dev/null; rm -f '$PROXY_PORT_FILE' '$CLIENT_CONFIG'" EXIT

for _ in {1..300}; do
    [ -s "$PROXY_PORT_FILE" ] && break
    sleep 0.1
done

PROXY_PORT=$(cat "$PROXY_PORT_FILE")
printf '<clickhouse><sync_request_timeout>0.5</sync_request_timeout></clickhouse>\n' > "$CLIENT_CONFIG"
CLIENT_OPT=$(echo "${CLICKHOUSE_CLIENT_OPT}" | sed "s/--host=[^ ]*//g; s/--port=[^ ]*//g")

# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT_BINARY} ${CLIENT_OPT} --send_logs_level=none --config-file "$CLIENT_CONFIG" --host 127.0.0.1 --port "$PROXY_PORT" --ignore-error --multiquery "
    CREATE TEMPORARY TABLE t_05182 ENGINE = Memory AS SELECT 1 AS x;
    SELECT throwIf(1);
    SELECT 'the query timeout kept the session' FROM t_05182 SETTINGS sync_request_timeout = 1.75;
" 2>&1 | grep -oF -e 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO' -e 'UNKNOWN_TABLE' -e 'NETWORK_ERROR' -e 'the query timeout kept the session'
