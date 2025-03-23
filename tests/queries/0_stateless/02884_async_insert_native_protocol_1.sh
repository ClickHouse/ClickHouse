#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_async_insert_native_1;
    CREATE TABLE t_async_insert_native_1 (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
"

async_insert_options="--async_insert 1 --wait_for_async_insert 0 --async_insert_busy_timeout_min_ms 1000000 --async_insert_busy_timeout_max_ms 10000000"

echo '{"id": 1, "s": "aaa"} {"id": 2, "s": "bbb"}' | $CLICKHOUSE_CLIENT $async_insert_options -q 'INSERT INTO t_async_insert_native_1 FORMAT JSONEachRow'
$CLICKHOUSE_CLIENT $async_insert_options  -q 'INSERT INTO t_async_insert_native_1 FORMAT JSONEachRow {"id": 3, "s": "ccc"}'

# Mixed inlined and external data is not supported.
echo '{"id": 1, "s": "aaa"}' \
    | $CLICKHOUSE_CLIENT $async_insert_options -q 'INSERT INTO t_async_insert_native_1 FORMAT JSONEachRow {"id": 2, "s": "bbb"}' 2>&1 \
    | grep -o "NOT_IMPLEMENTED"

$CLICKHOUSE_CLIENT -q "
    SELECT sum(length(entries.bytes)) FROM system.asynchronous_inserts
    WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_async_insert_native_1';

    SYSTEM FLUSH ASYNC INSERT QUEUE;

    SELECT * FROM t_async_insert_native_1 ORDER BY id;

    SYSTEM FLUSH LOGS;

    SELECT status, rows, data_kind, format
    FROM system.asynchronous_insert_log
    WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_async_insert_native_1'
    ORDER BY event_time_microseconds;

    DROP TABLE t_async_insert_native_1;
"
