#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS async_inserts"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_inserts (id UInt32, s String) ENGINE = Memory"

${CLICKHOUSE_CURL} -sS $url -d 'INSERT INTO async_inserts1 FORMAT JSONEachRow {"id": 1, "s": "a"}'  \
    | grep -o "Code: 60"

${CLICKHOUSE_CURL} -sS $url -d 'INSERT INTO async_inserts FORMAT BadFormat {"id": 1, "s": "a"}'  \
    | grep -o "Code: 73"

${CLICKHOUSE_CURL} -sS $url -d 'INSERT INTO async_inserts FORMAT Pretty {"id": 1, "s": "a"}'  \
    | grep -o "Code: 73"

${CLICKHOUSE_CURL} -sS $url -d 'INSERT INTO async_inserts (id, a) FORMAT JSONEachRow {"id": 1, "s": "a"}'  \
    | grep -o "Code: 16"

${CLICKHOUSE_CLIENT} -q "DROP TABLE async_inserts"
