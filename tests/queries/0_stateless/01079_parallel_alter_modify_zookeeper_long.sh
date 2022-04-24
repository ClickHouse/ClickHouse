#!/usr/bin/env bash
# Tags: long, zookeeper, no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./replication.lib
. "$CURDIR"/replication.lib

REPLICAS=5

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS concurrent_alter_mt_$i"
done

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT -nm --query "
        CREATE TABLE concurrent_alter_mt_$i (key UInt64, value1 UInt64, value2 Int32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/concurrent_alter_mt', '$i')
        ORDER BY key
        SETTINGS max_replicated_mutations_in_queue=1000, number_of_free_entries_in_pool_to_execute_mutation=0,max_replicated_merges_in_queue=1000"
done

$CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_alter_mt_1 SELECT number, number + 10, number from numbers(10)"
$CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_alter_mt_1 SELECT number, number + 10, number from numbers(10, 40)"

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA concurrent_alter_mt_$i"
done

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "SELECT SUM(value1) FROM concurrent_alter_mt_$i"
done

INITIAL_SUM=$($CLICKHOUSE_CLIENT --query "SELECT SUM(value1) FROM concurrent_alter_mt_1")

# This alters mostly requires not only metadata change
# but also conversion of data. Also they are all compatible
# between each other, so can be executed concurrently.
function correct_alter_thread()
{
    TYPES=(Float64 String UInt8 UInt32)
    REPLICA=$(($RANDOM % 5 + 1))
    TYPE=${TYPES[$RANDOM % ${#TYPES[@]} ]}
    $CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_alter_mt_$REPLICA MODIFY COLUMN value1 $TYPE SETTINGS replication_alter_partitions_sync=0"; # additionaly we don't wait anything for more heavy concurrency
    sleep 0.$RANDOM
}

# This thread add some data to table. After we finish we can check, that
# all our data have same types.
# insert queries will fail sometime because of wrong types.
function insert_thread()
{
    VALUES=(7.0 7 '7')
    REPLICA=$(($RANDOM % 5 + 1))
    VALUE=${VALUES[$RANDOM % ${#VALUES[@]} ]}
    $CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_alter_mt_$REPLICA VALUES($RANDOM, $VALUE, $VALUE)"
    sleep 0.$RANDOM
}

# Some select load, to be sure, that our selects work in concurrent execution with alters
function select_thread()
{
    REPLICA=$(($RANDOM % 5 + 1))
    $CLICKHOUSE_CLIENT --query "SELECT SUM(toUInt64(value1)) FROM concurrent_alter_mt_$REPLICA" 1>/dev/null
    sleep 0.$RANDOM
}


echo "Starting alters"
export -f correct_alter_thread
export -f insert_thread
export -f select_thread


TIMEOUT=30


# Selects should run successfully
clickhouse_client_loop_timeout $TIMEOUT select_thread &
clickhouse_client_loop_timeout $TIMEOUT select_thread &
clickhouse_client_loop_timeout $TIMEOUT select_thread &

clickhouse_client_loop_timeout $TIMEOUT correct_alter_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT correct_alter_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT correct_alter_thread 2> /dev/null &

clickhouse_client_loop_timeout $TIMEOUT insert_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT insert_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT insert_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT insert_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT insert_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT insert_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT insert_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT insert_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT insert_thread 2> /dev/null &

wait

echo "Finishing alters"

# This alter will finish all previous, but replica 1 maybe still not up-to-date.
# If query will throw something, than we will sleep 1 and retry. If timeout
# happened we will silently go out of loop and probably fail tests in the
# following for loop.
#
# 120 seconds is more than enough, but in rare cases for slow builds (debug,
# thread) it maybe necessary.
while [[ $(timeout 120 ${CLICKHOUSE_CLIENT} --query "ALTER TABLE concurrent_alter_mt_1 MODIFY COLUMN value1 String SETTINGS replication_alter_partitions_sync=2" 2>&1) ]]; do
    sleep 1
done

check_replication_consistency "concurrent_alter_mt_" "count(), sum(key), sum(cityHash64(value1)), sum(cityHash64(value2))"

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA concurrent_alter_mt_$i"
    $CLICKHOUSE_CLIENT --query "SELECT SUM(toUInt64(value1)) > $INITIAL_SUM FROM concurrent_alter_mt_$i"
    $CLICKHOUSE_CLIENT --query "SELECT COUNT() FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and is_done=0 and table = 'concurrent_alter_mt_$i'" # all mutations have to be done
    $CLICKHOUSE_CLIENT --query "SELECT * FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and is_done=0 and table = 'concurrent_alter_mt_$i'"
    $CLICKHOUSE_CLIENT --query "SELECT * FROM system.replication_queue WHERE table = 'concurrent_alter_mt_$i' and (type = 'ALTER_METADATA' or type = 'MUTATE_PART')"
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS concurrent_alter_mt_$i"
done
