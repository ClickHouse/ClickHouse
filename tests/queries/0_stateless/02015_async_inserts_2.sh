#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS async_inserts"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_inserts (id UInt32, s String) ENGINE = MergeTree ORDER BY id"

${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO async_inserts FORMAT CSV
1,"a"
2,"b"' &

${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO async_inserts FORMAT CSV
qqqqqqqqqqq' 2>&1 | grep -o "Code: 27" &

${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO async_inserts FORMAT CSV
4,"c"
3,"d"' &

wait

${CLICKHOUSE_CLIENT} -q "SELECT * FROM async_inserts ORDER BY id"
${CLICKHOUSE_CLIENT} -q "SELECT name, rows, level FROM system.parts WHERE table = 'async_inserts' AND database = '$CLICKHOUSE_DATABASE' ORDER BY name"

${CLICKHOUSE_CLIENT} -q "DROP TABLE async_inserts"
