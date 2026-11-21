#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT_CONFIG="${CLICKHOUSE_TMP}/05186_client_${CLICKHOUSE_DATABASE}.xml"
printf '<clickhouse><sync_request_timeout>0.5</sync_request_timeout></clickhouse>\n' > "$CLIENT_CONFIG"
trap "rm -f '$CLIENT_CONFIG'" EXIT

${CLICKHOUSE_CLIENT_BINARY} --config-file "$CLIENT_CONFIG" --host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT_TCP" --database "$CLICKHOUSE_DATABASE" --query "SELECT 1"
