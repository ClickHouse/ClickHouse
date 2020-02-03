#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh


for i in {1..2}; do
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS concurrent_alter_mt_$i"
done

for i in {1..2}; do
    $CLICKHOUSE_CLIENT --query "CREATE TABLE concurrent_alter_mt_$i (key UInt64, value1 UInt64, value2 String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/concurrent_alter_mt', '$i') ORDER BY key"
done

$CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_alter_mt_1 SELECT number, number + 10, toString(number) from numbers(10)"
$CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_alter_mt_1 SELECT number, number + 10, toString(number) from numbers(10, 40)"

for i in {1..2}; do
    $CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA concurrent_alter_mt_$i"
done

for i in {1..2}; do
    $CLICKHOUSE_CLIENT --query "SELECT SUM(value1) FROM concurrent_alter_mt_$i"
done

INITIAL_SUM=`$CLICKHOUSE_CLIENT --query "SELECT SUM(value1) FROM concurrent_alter_mt_1"`

# This is just garbage thread with conflictings alter
# it additionally loads alters "queue".
function garbage_alter_thread()
{
    while true; do
        REPLICA=$(($RANDOM % 2 + 1))
        $CLICKHOUSE_CLIENT -n --query "ALTER TABLE concurrent_alter_mt_$REPLICA ADD COLUMN h String DEFAULT '0'; ALTER TABLE concurrent_alter_mt_$REPLICA MODIFY COLUMN h UInt64; ALTER TABLE concurrent_alter_mt_$REPLICA DROP COLUMN h;";
    done
}


# This alters mostly requires not only metadata change
# but also conversion of data. Also they are all compatible
# between each other, so can be executed concurrently.
function correct_alter_thread()
{
    TYPES=(Float64 String UInt8 UInt32)
    while true; do
        REPLICA=$(($RANDOM % 2 + 1))
        TYPE=${TYPES[$RANDOM % ${#TYPES[@]} ]}
        $CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_alter_mt_$REPLICA MODIFY COLUMN value1 $TYPE SETTINGS replication_alter_partitions_sync=0"; # additionaly we don't wait anything for more heavy concurrency
        sleep 0.$RANDOM
    done
}

# This thread add some data to table. After we finish we can check, that
# all our data have same types.
# insert queries will fail sometime because of wrong types.
function insert_thread()
{

    VALUES=(7.0 7 '7')
    while true; do
        REPLICA=$(($RANDOM % 2 + 1))
        VALUE=${VALUES[$RANDOM % ${#VALUES[@]} ]}
        $CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_alter_mt_$REPLICA VALUES($RANDOM, $VALUE, toString($VALUE))"
        sleep 0.$RANDOM
    done
}

echo "Starting alters"

export -f garbage_alter_thread;
export -f correct_alter_thread;
export -f insert_thread;

TIMEOUT=10

timeout $TIMEOUT bash -c garbage_alter_thread 2> /dev/null &
timeout $TIMEOUT bash -c garbage_alter_thread 2> /dev/null &
timeout $TIMEOUT bash -c garbage_alter_thread 2> /dev/null &
#timeout $TIMEOUT bash -c garbage_alter_thread 2> /dev/null &
#timeout $TIMEOUT bash -c garbage_alter_thread 2> /dev/null &

timeout $TIMEOUT bash -c correct_alter_thread 2> /dev/null &
timeout $TIMEOUT bash -c correct_alter_thread 2> /dev/null &
timeout $TIMEOUT bash -c correct_alter_thread 2> /dev/null &
#timeout $TIMEOUT bash -c correct_alter_thread 2> /dev/null &
#timeout $TIMEOUT bash -c correct_alter_thread 2> /dev/null &

# We don't want too many parts, just several alters per second
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
#timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
#timeout $TIMEOUT bash -c insert_thread 2> /dev/null &

wait

echo "Finishing alters"

# This alter will finish all previous
$CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_alter_mt_1 MODIFY COLUMN value1 String"

for i in {1..2}; do
    $CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA concurrent_alter_mt_$i"
done

for i in {1..2}; do
    $CLICKHOUSE_CLIENT --query "SELECT SUM(toUInt64(value1)) > $INITIAL_SUM FROM concurrent_alter_mt_$i"
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS concurrent_alter_mt_$i"
done
