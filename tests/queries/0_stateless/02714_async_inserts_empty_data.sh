#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_async_insert_empty_data"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_async_insert_empty_data (id UInt32) ENGINE = Memory"

echo -n '' | ${CLICKHOUSE_CURL} -sS "$url&query=INSERT%20INTO%20t_async_insert_empty_data%20FORMAT%20JSONEachRow" --data-binary @-

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_async_insert_empty_data"
${CLICKHOUSE_CLIENT} -q "SELECT status, bytes FROM system.asynchronous_insert_log WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_async_insert_empty_data'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_async_insert_empty_data"
