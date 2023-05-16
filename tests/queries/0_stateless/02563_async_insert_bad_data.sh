#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_async_insert_bad_data"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_async_insert_bad_data (d Date, s String) ENGINE = Memory"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1" -X POST \
    --data-binary "INSERT INTO t_async_insert_bad_data VALUES (now(), 'bad'string')" 2>&1 | grep -o SYNTAX_ERROR &

# Sleep to avoid reordering of inserts. It doesn't affect the correctness
# of test, but allows to reproduce a bug more often in old versions.
sleep 0.02

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1" -X POST  \
    --data-binary "INSERT INTO t_async_insert_bad_data VALUES (now(), 'good string')" &

wait

${CLICKHOUSE_CLIENT} -q "SELECT s FROM t_async_insert_bad_data ORDER BY s"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_async_insert_bad_data"
