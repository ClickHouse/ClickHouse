#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest, no-backward-compatibility-check

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

REPLICAS=3

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS concurrent_alter_detach_$i"
done

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT -nm --query "
        CREATE TABLE concurrent_alter_detach_$i (key UInt64, value1 UInt8, value2 UInt8)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/concurrent_alter_detach', '$i')
        ORDER BY key
        SETTINGS
            max_replicated_mutations_in_queue=1000,
            number_of_free_entries_in_pool_to_execute_mutation=0,
            max_replicated_merges_in_queue=1000,
            temporary_directories_lifetime=10,
            cleanup_delay_period=3,
            cleanup_delay_period_random_add=0;
    "
done

$CLICKHOUSE_CLIENT -nm --query "
    INSERT INTO concurrent_alter_detach_1 SELECT number, number + 10, number from numbers(10);
    INSERT INTO concurrent_alter_detach_1 SELECT number, number + 10, number from numbers(10, 40);
"

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA concurrent_alter_detach_$i"
    $CLICKHOUSE_CLIENT --query "SELECT SUM(value1) FROM concurrent_alter_detach_$i"
done

INITIAL_SUM=$($CLICKHOUSE_CLIENT --query "SELECT SUM(value1) FROM concurrent_alter_detach_1")

# This alters mostly requires not only metadata change
# but also conversion of data. Also they are all compatible
# between each other, so can be executed concurrently.
function correct_alter_thread()
{
    TYPES=(Float64 String UInt8 UInt32)
    REPLICA=$(($RANDOM % 3 + 1))
    TYPE=${TYPES[$RANDOM % ${#TYPES[@]} ]}
    # additionaly we don't wait anything for more heavy concurrency
    $CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_alter_detach_$REPLICA MODIFY COLUMN value1 $TYPE SETTINGS replication_alter_partitions_sync=0"
    sleep 0.$RANDOM
}

# This thread add some data to table. After we finish we can check, that
# all our data have same types.
# insert queries will fail sometime because of wrong types.
function insert_thread()
{
    VALUES=(7.0 7 '7')
    REPLICA=$(($RANDOM % 3 + 1))
    VALUE=${VALUES[$RANDOM % ${#VALUES[@]} ]}
    $CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_alter_detach_$REPLICA VALUES($RANDOM, $VALUE, $VALUE)"
    sleep 0.$RANDOM
}

function detach_attach_thread()
{
    REPLICA=$(($RANDOM % 3 + 1))
    $CLICKHOUSE_CLIENT --query "DETACH TABLE concurrent_alter_detach_$REPLICA"
    sleep 0.$RANDOM
    $CLICKHOUSE_CLIENT --query "ATTACH TABLE concurrent_alter_detach_$REPLICA"
}

echo "Starting alters"
export -f correct_alter_thread
export -f insert_thread
export -f detach_attach_thread

TIMEOUT=15

# Sometimes we detach and attach tables
clickhouse_client_loop_timeout $TIMEOUT detach_attach_thread 2> /dev/null &

clickhouse_client_loop_timeout $TIMEOUT correct_alter_thread 2> /dev/null &

clickhouse_client_loop_timeout $TIMEOUT insert_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT insert_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT insert_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT insert_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT insert_thread 2> /dev/null &

wait

echo "Finishing alters"

sleep 1

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "ATTACH TABLE concurrent_alter_detach_$i" 2> /dev/null
done

# This alter will finish all previous, but replica 1 maybe still not up-to-date
while [[ $(timeout 120 ${CLICKHOUSE_CLIENT} --query "ALTER TABLE concurrent_alter_detach_1 MODIFY COLUMN value1 String SETTINGS replication_alter_partitions_sync=2" 2>&1) ]]; do
    sleep 1
    # just try to attach table if it failed for some reason in the code above
    for i in $(seq $REPLICAS); do
        $CLICKHOUSE_CLIENT --query "ATTACH TABLE concurrent_alter_detach_$i" 2> /dev/null
    done
done

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA concurrent_alter_detach_$i"
    $CLICKHOUSE_CLIENT --query "SELECT SUM(toUInt64(value1)) > $INITIAL_SUM FROM concurrent_alter_detach_$i"
    $CLICKHOUSE_CLIENT --query "SELECT COUNT() FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and is_done=0 and table = 'concurrent_alter_detach_$i'" # all mutations have to be done
    $CLICKHOUSE_CLIENT --query "SELECT * FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and is_done=0 and table = 'concurrent_alter_detach_$i'" # all mutations have to be done
    $CLICKHOUSE_CLIENT --query "SELECT * FROM system.replication_queue WHERE table = 'concurrent_alter_detach_$i' and (type = 'ALTER_METADATA' or type = 'MUTATE_PART')" # all mutations and alters have to be done
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS concurrent_alter_detach_$i"
done
