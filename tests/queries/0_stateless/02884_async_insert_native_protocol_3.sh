#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_async_insert_native_3;
    CREATE TABLE t_async_insert_native_3 (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
"

async_insert_options="--async_insert 1 --wait_for_async_insert 0 --async_insert_busy_timeout_min_ms 1000000 --async_insert_busy_timeout_max_ms 10000000"

echo '{"id": 1, "s": "aaa"} {"id": 2, "s": "bbb"}' | $CLICKHOUSE_CLIENT $async_insert_options -q 'INSERT INTO t_async_insert_native_3 FORMAT JSONEachRow'
echo "(3, 'ccc') (4, 'ddd') (5, 'eee')" | $CLICKHOUSE_CLIENT $async_insert_options -q 'INSERT INTO t_async_insert_native_3 FORMAT Values'
echo '6,"fff"' | $CLICKHOUSE_CLIENT $async_insert_options -q 'INSERT INTO t_async_insert_native_3 FORMAT CSV'
$CLICKHOUSE_CLIENT $async_insert_options -q "INSERT INTO t_async_insert_native_3 VALUES (7, 'ggg')"

wait

$CLICKHOUSE_CLIENT -q "
    SELECT format, length(entries.bytes) FROM system.asynchronous_inserts
    WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_async_insert_native_3'
    ORDER BY format;

    SYSTEM FLUSH ASYNC INSERT QUEUE;

    SELECT * FROM t_async_insert_native_3 ORDER BY id;

    SYSTEM FLUSH LOGS;

    SELECT format, status, rows, data_kind, format
    FROM system.asynchronous_insert_log
    WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_async_insert_native_3'
    ORDER BY event_time_microseconds;

    DROP TABLE t_async_insert_native_3;
"
