#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

REPLICAS=3

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS concurrent_alter_add_drop_$i"
done


for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "CREATE TABLE concurrent_alter_add_drop_$i (key UInt64, value0 UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/concurrent_alter_add_drop_column', '$i') ORDER BY key SETTINGS max_replicated_mutations_in_queue=1000, number_of_free_entries_in_pool_to_execute_mutation=0,max_replicated_merges_in_queue=1000"
done

$CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_alter_add_drop_1 SELECT number, number + 10 from numbers(100000)"

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA concurrent_alter_add_drop_$i"
done


function alter_thread()
{
    while true; do
        REPLICA=$(($RANDOM % 3 + 1))
        ADD=$(($RANDOM % 5 + 1))
        $CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_alter_add_drop_$REPLICA ADD COLUMN value$ADD UInt32 DEFAULT 42 SETTINGS replication_alter_partitions_sync=0"; # additionaly we don't wait anything for more heavy concurrency
        DROP=$(($RANDOM % 5 + 1))
        $CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_alter_add_drop_$REPLICA DROP COLUMN value$DROP SETTINGS replication_alter_partitions_sync=0"; # additionaly we don't wait anything for more heavy concurrency
        sleep 0.$RANDOM
    done
}


function optimize_thread()
{
    while true; do
        REPLICA=$(($RANDOM % 3 + 1))
        $CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE concurrent_alter_add_drop_$REPLICA FINAL SETTINGS replication_alter_partitions_sync=0";
        sleep 0.$RANDOM
    done
}

function insert_thread()
{
    while true; do
        REPLICA=$(($RANDOM % 3 + 1))
        $CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_alter_add_drop_$REPLICA VALUES($RANDOM, 7)"
        sleep 0.$RANDOM
    done
}


echo "Starting alters"
export -f alter_thread;
export -f optimize_thread;
export -f insert_thread;


TIMEOUT=30

# Sometimes we detach and attach tables
timeout $TIMEOUT bash -c alter_thread 2> /dev/null &
timeout $TIMEOUT bash -c alter_thread 2> /dev/null &
timeout $TIMEOUT bash -c alter_thread 2> /dev/null &

timeout $TIMEOUT bash -c optimize_thread 2> /dev/null &
timeout $TIMEOUT bash -c optimize_thread 2> /dev/null &
timeout $TIMEOUT bash -c optimize_thread 2> /dev/null &

timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &


wait

echo "Finishing alters"

columns1=$($CLICKHOUSE_CLIENT --query "select count() from system.columns where table='concurrent_alter_add_drop_1'" 2> /dev/null)
columns2=$($CLICKHOUSE_CLIENT --query "select count() from system.columns where table='concurrent_alter_add_drop_2'" 2> /dev/null)
columns3=$($CLICKHOUSE_CLIENT --query "select count() from system.columns where table='concurrent_alter_add_drop_3'" 2> /dev/null)

while [ "$columns1" != "$columns2" ] || [ "$columns2" != "$columns3" ]; do
    columns1=$($CLICKHOUSE_CLIENT --query "select count() from system.columns where table='concurrent_alter_add_drop_1'" 2> /dev/null)
    columns2=$($CLICKHOUSE_CLIENT --query "select count() from system.columns where table='concurrent_alter_add_drop_2'" 2> /dev/null)
    columns3=$($CLICKHOUSE_CLIENT --query "select count() from system.columns where table='concurrent_alter_add_drop_3'" 2> /dev/null)

    sleep 1
done

echo "Equal number of columns"

# This alter will finish all previous, but replica 1 maybe still not up-to-date
while [[ $(timeout 120 ${CLICKHOUSE_CLIENT} --query "ALTER TABLE concurrent_alter_add_drop_1 MODIFY COLUMN value0 String SETTINGS replication_alter_partitions_sync=2" 2>&1) ]]; do
    sleep 1
done

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA concurrent_alter_add_drop_$i"
    $CLICKHOUSE_CLIENT --query "SELECT COUNT() FROM system.mutations WHERE is_done = 0 and table = 'concurrent_alter_add_drop_$i'"
    $CLICKHOUSE_CLIENT --query "SELECT * FROM system.mutations WHERE is_done = 0 and table = 'concurrent_alter_add_drop_$i'"
    $CLICKHOUSE_CLIENT --query "SELECT COUNT() FROM system.replication_queue WHERE table = 'concurrent_alter_add_drop_$i'"
    $CLICKHOUSE_CLIENT --query "SELECT * FROM system.replication_queue WHERE table = 'concurrent_alter_add_drop_$i' and (type = 'ALTER_METADATA' or type = 'MUTATE_PART')"
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS concurrent_alter_add_drop_$i"
done
