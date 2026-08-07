#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS async_inserts"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_inserts (id UInt32, v UInt32 DEFAULT id * id) ENGINE = Memory"

${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO async_inserts FORMAT CSV
1,
2,' &

${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO async_inserts FORMAT CSV
3,
4,
' &

${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO async_inserts FORMAT JSONEachRow {"id": 5} {"id": 6}' &
${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO async_inserts FORMAT JSONEachRow {"id": 7} {"id": 8}' &

wait

${CLICKHOUSE_CLIENT} -q "SELECT * FROM async_inserts ORDER BY id"

${CLICKHOUSE_CLIENT} -q "DROP TABLE async_inserts"
