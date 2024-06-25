#!/usr/bin/env bash
# Tags: race, zookeeper, no-parallel, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

NUM_REPLICAS=5

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -n -q "
        DROP TABLE IF EXISTS r$i SYNC;
        CREATE TABLE r$i (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r', 'r$i') ORDER BY x SETTINGS replicated_deduplication_window = 1, allow_remote_fs_zero_copy_replication = 1;
    "
done

function thread {
    while true; do
        REPLICA=$(($RANDOM % 5 + 1))
        $CLICKHOUSE_CLIENT --query "INSERT INTO r$REPLICA SELECT rand()"
    done
}

function nemesis_thread1 {
    while true; do
        REPLICA=$(($RANDOM % 5 + 1))
        $CLICKHOUSE_CLIENT --query "SYSTEM STOP REPLICATED SENDS r$REPLICA"
        sleep 0.5
        $CLICKHOUSE_CLIENT --query "SYSTEM START REPLICATED SENDS r$REPLICA"
    done
}

function nemesis_thread2 {
    while true; do
        REPLICA=$(($RANDOM % 5 + 1))
        $CLICKHOUSE_CLIENT --query "SYSTEM STOP FETCHES r$REPLICA"
        sleep 0.5
        $CLICKHOUSE_CLIENT --query "SYSTEM START FETCHES r$REPLICA"
    done
}




TIMEOUT=20

spawn_with_timeout $TIMEOUT thread 2>/dev/null
spawn_with_timeout $TIMEOUT thread 2>/dev/null
spawn_with_timeout $TIMEOUT thread 2>/dev/null
spawn_with_timeout $TIMEOUT nemesis_thread1 2>/dev/null
spawn_with_timeout $TIMEOUT nemesis_thread1 2>/dev/null
spawn_with_timeout $TIMEOUT nemesis_thread1 2>/dev/null
spawn_with_timeout $TIMEOUT nemesis_thread2 2>/dev/null
spawn_with_timeout $TIMEOUT nemesis_thread2 2>/dev/null
spawn_with_timeout $TIMEOUT nemesis_thread2 2>/dev/null

wait


for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -q "SYSTEM START FETCHES r$REPLICA"
    $CLICKHOUSE_CLIENT -q "SYSTEM START REPLICATED SENDS r$REPLICA"
done

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT --max_execution_time 60 -q "SYSTEM SYNC REPLICA r$i PULL"
done

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -q "DROP TABLE r$i" 2>/dev/null &
done

wait
