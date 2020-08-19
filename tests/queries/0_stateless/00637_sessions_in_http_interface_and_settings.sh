#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

user="readonly"
address=${CLICKHOUSE_HOST}
port=${CLICKHOUSE_PORT_HTTP}
url="${CLICKHOUSE_PORT_HTTP_PROTO}://${user}@${address}:${port}/?session_id=test"
select="SELECT name, value, changed FROM system.settings WHERE name = 'readonly'"

${CLICKHOUSE_CURL} -sS "$url" --data-binary "$select"
