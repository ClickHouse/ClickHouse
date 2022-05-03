#!/usr/bin/env bash
# Tags: long, no-parallel, no-s3-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test"

$CLICKHOUSE_CLIENT --query "CREATE TABLE test (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='s3_cache', min_bytes_for_wide_part = 0"

$CLICKHOUSE_CLIENT --query "INSERT INTO test SELECT * FROM generateRandom('key UInt32, value String') LIMIT 1000000"

function select_thread()
{
    $CLICKHOUSE_CLIENT --query "SELECT * FROM test FORMAT Null"
}

function drop_thread()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE FORCE"
}

export -f select_thread
export -f drop_thread

TIMEOUT=30

clickhouse_client_loop_timeout $TIMEOUT select_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT select_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT select_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT select_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT select_thread 2> /dev/null &

clickhouse_client_loop_timeout $TIMEOUT drop_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT drop_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT drop_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT drop_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT drop_thread 2> /dev/null &

wait

$CLICKHOUSE_CLIENT --query "SELECT 1"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test"
