#!/usr/bin/env bash
# Tags: deadlock, replica, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for i in $(seq 4); do
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS replica_01108_$i"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS replica_01108_${i}_tmp"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE replica_01108_$i (n int) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/replica_01108_$i', 'replica') ORDER BY tuple()"
    $CLICKHOUSE_CLIENT -q "INSERT INTO replica_01108_$i SELECT * FROM system.numbers LIMIT $i * 10, 10"
done

function rename_thread_1()
{
    $CLICKHOUSE_CLIENT -q "RENAME TABLE replica_01108_1 TO replica_01108_1_tmp,
                                        replica_01108_2 TO replica_01108_2_tmp,
                                        replica_01108_3 TO replica_01108_3_tmp,
                                        replica_01108_4 TO replica_01108_4_tmp"
    sleep 0.$RANDOM
}

function rename_thread_2()
{
    $CLICKHOUSE_CLIENT -q "RENAME TABLE replica_01108_1_tmp TO replica_01108_2,
                                        replica_01108_2_tmp TO replica_01108_3,
                                        replica_01108_3_tmp TO replica_01108_4,
                                        replica_01108_4_tmp TO replica_01108_1"
    sleep 0.$RANDOM
}

function restart_replicas_loop()
{
    for i in $(seq 4); do
        $CLICKHOUSE_CLIENT -q "SYSTEM RESTART REPLICA replica_01108_${i}"
        $CLICKHOUSE_CLIENT -q "SYSTEM RESTART REPLICA replica_01108_${i}_tmp"
    done
    sleep 0.$RANDOM
}

export -f rename_thread_1
export -f rename_thread_2
export -f restart_replicas_loop

TIMEOUT=10

clickhouse_client_loop_timeout $TIMEOUT rename_thread_1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT rename_thread_2 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT restart_replicas_loop 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT restart_replicas_loop 2> /dev/null &

wait

for i in $(seq 4); do
    $CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA replica_01108_$i" >/dev/null 2>&1
    $CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA replica_01108_${i}_tmp" >/dev/null 2>&1
done

while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query LIKE 'RENAME%'") -gt 0 ]]; do
    sleep 1
done;

$CLICKHOUSE_CLIENT -q "SELECT replaceOne(name, '_tmp', '') FROM system.tables WHERE database = '$CLICKHOUSE_DATABASE' AND match(name, '^replica_01108_')"
$CLICKHOUSE_CLIENT -q "SELECT sum(n), count(n) FROM merge('$CLICKHOUSE_DATABASE', '^replica_01108_') GROUP BY position(_table, 'tmp')"


for i in $(seq 4); do
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS replica_01108_$i"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS replica_01108_${i}_tmp"
done
