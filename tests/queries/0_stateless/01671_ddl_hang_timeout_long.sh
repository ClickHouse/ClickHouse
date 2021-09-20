#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function thread_create_drop_table {
    while true; do
        REPLICA=$(($RANDOM % 10))
        $CLICKHOUSE_CLIENT --query "CREATE TABLE IF NOT EXISTS t1 (x UInt64, s Array(Nullable(String))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_01671', 'r_$REPLICA') order by x" 2>/dev/null
        sleep 0.0$RANDOM
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t1"
    done
}

function thread_alter_table {
    while true; do
        $CLICKHOUSE_CLIENT --query "ALTER TABLE $CLICKHOUSE_DATABASE.t1 on cluster test_shard_localhost ADD COLUMN newcol UInt32" >/dev/null 2>&1
        sleep 0.0$RANDOM
    done
}

export -f thread_create_drop_table
export -f thread_alter_table
timeout 20 bash -c "thread_create_drop_table" &
timeout 20 bash -c 'thread_alter_table' &
wait
sleep 1

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t1";
