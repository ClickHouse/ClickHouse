#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_async_insert_native_2;
    CREATE TABLE t_async_insert_native_2 (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
"

async_insert_options="--async_insert 1 --wait_for_async_insert 1"

echo '{"id": 1, "s": "aaa"} {"id": 2, "s": "bbb"}' | $CLICKHOUSE_CLIENT $async_insert_options -q 'INSERT INTO t_async_insert_native_2 FORMAT JSONEachRow' &
echo "(3, 'ccc') (4, 'ddd') (5, 'eee')" | $CLICKHOUSE_CLIENT $async_insert_options -q 'INSERT INTO t_async_insert_native_2 FORMAT Values' &

wait

$CLICKHOUSE_CLIENT -q "
    SELECT * FROM t_async_insert_native_2 ORDER BY id;

    SYSTEM FLUSH LOGS;

    SELECT format, status, rows, data_kind, format
    FROM system.asynchronous_insert_log
    WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_async_insert_native_2'
    ORDER BY format;

    DROP TABLE t_async_insert_native_2;
"
