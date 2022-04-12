#!/usr/bin/env bash
# Tags: shard, no-fasttest

# This test reproduces crash in case of insufficient coroutines stack size

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS local"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS distributed"

$CLICKHOUSE_CLIENT --query "CREATE TABLE local (x UInt8) ENGINE = Memory;"
$CLICKHOUSE_CLIENT --query "CREATE TABLE distributed AS local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), local, x);"

$CLICKHOUSE_CLIENT --insert_distributed_sync=0 --network_compression_method='zstd' --query "INSERT INTO distributed SELECT number FROM numbers(256);"
$CLICKHOUSE_CLIENT --insert_distributed_sync=0 --network_compression_method='zstd' --query "SYSTEM FLUSH DISTRIBUTED distributed;"

function select_thread()
{
    $CLICKHOUSE_CLIENT --insert_distributed_sync=0 --network_compression_method='zstd' --query "SELECT count() FROM local" >/dev/null
    $CLICKHOUSE_CLIENT --insert_distributed_sync=0 --network_compression_method='zstd' --query "SELECT count() FROM distributed" >/dev/null
}

export -f select_thread

TIMEOUT=30

clickhouse_client_loop_timeout $TIMEOUT select_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT select_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT select_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT select_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT select_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT select_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT select_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT select_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT select_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT select_thread 2> /dev/null &

wait

$CLICKHOUSE_CLIENT --query "SELECT 1"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS local"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS distributed"
