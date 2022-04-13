#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS async_inserts_2156"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_inserts_2156 (id UInt32, s String) ENGINE = Memory"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0" -d "INSERT INTO async_inserts_2156 VALUES (1, 'a')"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1" -d "INSERT INTO async_inserts_2156 VALUES (2, 'b')"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM async_inserts_2156 ORDER BY id"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"

${CLICKHOUSE_CLIENT} -q "SELECT query, arrayExists(x -> x LIKE '%async_inserts_2156', tables), \
        query_kind, Settings['async_insert'], Settings['wait_for_async_insert'] FROM system.query_log \
    WHERE event_date >= yesterday() AND current_database = '$CLICKHOUSE_DATABASE' \
    AND query ILIKE 'INSERT INTO async_inserts_2156 VALUES%' AND type = 'QueryFinish' \
    ORDER BY query_start_time_microseconds"

${CLICKHOUSE_CLIENT} -q "DROP TABLE async_inserts_2156"
