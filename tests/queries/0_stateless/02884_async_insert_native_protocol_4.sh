#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_async_insert_native_4;
    CREATE TABLE t_async_insert_native_4 (id UInt64) ENGINE = MergeTree ORDER BY id;
"

async_insert_options="--async_insert 1 --wait_for_async_insert 1"
CLICKHOUSE_CLIENT_WITH_LOG=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=trace/g')

echo "(1)" | $CLICKHOUSE_CLIENT_WITH_LOG $async_insert_options --async_insert_max_data_size 10 \
     -q 'INSERT INTO t_async_insert_native_4 FORMAT Values' 2>&1 \
     | grep -c "too much data"

echo "(2) (3) (4) (5)" | $CLICKHOUSE_CLIENT_WITH_LOG $async_insert_options --async_insert_max_data_size 10 \
    -q 'INSERT INTO t_async_insert_native_4 FORMAT Values' 2>&1 \
    | grep -c "too much data"

$CLICKHOUSE_CLIENT -q "
    SELECT * FROM t_async_insert_native_4 ORDER BY id;

    SYSTEM FLUSH LOGS;

    SELECT format, status, rows, data_kind, format
    FROM system.asynchronous_insert_log
    WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_async_insert_native_4'
    ORDER BY format;

    DROP TABLE t_async_insert_native_4;
"
