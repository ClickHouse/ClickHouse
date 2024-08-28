#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_async_insert_params;
    CREATE TABLE t_async_insert_params (id UInt64) ENGINE = Memory;
"
cmd_params="--async_insert 1 --async_insert_busy_timeout_max_ms 300000 --async_insert_busy_timeout_min_ms 300000 --wait_for_async_insert 0 --async_insert_use_adaptive_busy_timeout 0"

$CLICKHOUSE_CLIENT $cmd_params -q "SET param_p1 = 11; INSERT INTO t_async_insert_params VALUES ({p1:UInt64});"
$CLICKHOUSE_CLIENT $cmd_params -q "SET param_p2 = 12; INSERT INTO t_async_insert_params VALUES ({p2:UInt64});"
$CLICKHOUSE_CLIENT $cmd_params -q "SET param_p2 = 1000; INSERT INTO t_async_insert_params VALUES (13);"
$CLICKHOUSE_CLIENT $cmd_params -q 'SET param_p2 = 1000; INSERT INTO t_async_insert_params FORMAT JSONEachRow {"id": 14};'

$CLICKHOUSE_CLIENT $cmd_params --param_p1 15 -q "INSERT INTO t_async_insert_params VALUES ({p1:UInt64});"
$CLICKHOUSE_CLIENT $cmd_params --param_p2 16 -q "INSERT INTO t_async_insert_params VALUES ({p2:UInt64});"
$CLICKHOUSE_CLIENT $cmd_params --param_p2 1000 -q "INSERT INTO t_async_insert_params VALUES (17);"
$CLICKHOUSE_CLIENT $cmd_params --param_p2 1000 -q 'INSERT INTO t_async_insert_params FORMAT JSONEachRow {"id": 18};'

url="${CLICKHOUSE_URL}&async_insert=1&async_insert_busy_timeout_max_ms=300000&async_insert_busy_timeout_min_ms=300000&wait_for_async_insert=0&async_insert_use_adaptive_busy_timeout=0"

${CLICKHOUSE_CURL} -sS "$url&param_p1=19" -d "INSERT INTO t_async_insert_params VALUES ({p1:UInt64})"
${CLICKHOUSE_CURL} -sS "$url&param_p2=20" -d "INSERT INTO t_async_insert_params VALUES ({p2:UInt64})"
${CLICKHOUSE_CURL} -sS "$url&param_p3=21" -d "INSERT INTO t_async_insert_params VALUES ({p3:UInt64})"
${CLICKHOUSE_CURL} -sS "$url&param_p2=1000" -d "INSERT INTO t_async_insert_params VALUES (22)"
${CLICKHOUSE_CURL} -sS "$url&param_p2=1000" -d 'INSERT INTO t_async_insert_params FORMAT JSONEachRow {"id": 23}'

$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH ASYNC INSERT QUEUE;
    SELECT id FROM t_async_insert_params ORDER BY id;
    DROP TABLE IF EXISTS t_async_insert_params;
"
