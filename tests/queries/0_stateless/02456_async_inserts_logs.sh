#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_async_inserts_logs"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_async_inserts_logs (id UInt32, s String) ENGINE = MergeTree ORDER BY id"

${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO t_async_inserts_logs FORMAT JSONEachRow {"id": 5, "s": "e"} {"id": 6, "s": "f"}' &
${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO t_async_inserts_logs VALUES (1, 'a')" &

${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO t_async_inserts_logs FORMAT JSONEachRow qqqqqq' > /dev/null 2>&1 &
${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO t_async_inserts_logs VALUES qqqqqq' > /dev/null 2>&1 &

${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), t_async_inserts_logs) VALUES (1, 'aaa') (2, 'bbb')" &

wait

${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_async_inserts_logs FINAL"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_async_inserts_logs MODIFY SETTING parts_to_throw_insert = 1"

${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO t_async_inserts_logs VALUES (1, 'a')" > /dev/null 2>&1 &

wait

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_async_inserts_logs"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} -q "
    SELECT table, format, bytes, rows, empty(exception), status,
    status = 'ParsingError' ? flush_time_microseconds = 0 : flush_time_microseconds > event_time_microseconds AS time_ok
    FROM system.asynchronous_insert_log
    WHERE
    (
        database = '$CLICKHOUSE_DATABASE' AND table = 't_async_inserts_logs'
        OR query ILIKE 'INSERT INTO FUNCTION%$CLICKHOUSE_DATABASE%t_async_inserts_logs%'
    )
    AND data_kind = 'Parsed'
    ORDER BY table, status, format"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_async_inserts_logs"

${CLICKHOUSE_CLIENT} -q "
SELECT event, value > 0 FROM system.events
WHERE event IN ('AsyncInsertQuery', 'AsyncInsertBytes', 'AsyncInsertRows')
ORDER BY event"
