#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS test_01671"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE test_01671"
function thread_create_drop_table {
    while true; do
        REPLICA=$(($RANDOM % 10))
        $CLICKHOUSE_CLIENT --query "CREATE TABLE IF NOT EXISTS test_01671.t1 (x UInt64, s Array(Nullable(String))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_01671/test_01671', 'r_$REPLICA') order by x" 2>/dev/null
        sleep 0.0$RANDOM
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_01671.t1"
    done
}
function thread_alter_table {
    while true; do
        $CLICKHOUSE_CLIENT --query "ALTER TABLE test_01671.t1 on cluster test_shard_localhost ADD COLUMN newcol UInt32" >/dev/null 2>&1
        sleep 0.0$RANDOM
    done
}
export -f thread_create_drop_table
export -f thread_alter_table
timeout 20 bash -c "thread_create_drop_table" &
timeout 20 bash -c 'thread_alter_table' &
wait
sleep 1

$CLICKHOUSE_CLIENT --query "DROP DATABASE test_01671";
