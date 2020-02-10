#!/usr/bin/env bash


# This test checks mutations concurrent execution with concurrent inserts.
# There was a bug in mutations finalization, when mutation finishes not after all
# MUTATE_PART tasks execution, but after GET of already mutated part from other replica.
# To test it we stop replicated sends on some replicas to delay fetch of required parts for mutation.
# Since our replication queue executing tasks concurrently it may happen, that we dowload already mutated
# part before source part.


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

REPLICAS=5

for i in `seq $REPLICAS`; do
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS concurrent_alter_mt_$i"
done

for i in `seq $REPLICAS`; do
    $CLICKHOUSE_CLIENT --query "CREATE TABLE concurrent_alter_mt_$i (key UInt64, value1 UInt64, value2 String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/concurrent_alter_mt', '$i') ORDER BY key"
done

$CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_alter_mt_1 SELECT number, number + 10, toString(number) from numbers(10)"
$CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_alter_mt_1 SELECT number, number + 10, toString(number) from numbers(10, 40)"

for i in `seq $REPLICAS`; do
    $CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA concurrent_alter_mt_$i"
done

for i in `seq $REPLICAS`; do
    $CLICKHOUSE_CLIENT --query "SELECT SUM(value1) FROM concurrent_alter_mt_$i"
done

INITIAL_SUM=`$CLICKHOUSE_CLIENT --query "SELECT SUM(value1) FROM concurrent_alter_mt_1"`

# Run mutation on random replica
>>>>>>> master
function correct_alter_thread()
{
    TYPES=(Float64 String UInt8 UInt32)
    while true; do
        REPLICA=$(($RANDOM % 5 + 1))
        TYPE=${TYPES[$RANDOM % ${#TYPES[@]} ]}
        $CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_alter_mt_$REPLICA UPDATE value1 = value1 + 1 WHERE 1";
        sleep 0.$RANDOM
    done
}

# This thread add some data to table.
function insert_thread()
{

    VALUES=(7 8 9)
>>>>>>> master
    while true; do
        REPLICA=$(($RANDOM % 5 + 1))
        VALUE=${VALUES[$RANDOM % ${#VALUES[@]} ]}
        $CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_alter_mt_$REPLICA VALUES($RANDOM, $VALUE, toString($VALUE))"
        sleep 0.$RANDOM
    done
}

# Stop sends for some times on random replica
function stop_sends_thread()
{

    while true; do
        REPLICA=$(($RANDOM % 5 + 1))
        $CLICKHOUSE_CLIENT --query "SYSTEM STOP REPLICATED SENDS concurrent_alter_mt_$REPLICA"
        sleep 0.$RANDOM
        sleep 0.$RANDOM
        sleep 0.$RANDOM
        $CLICKHOUSE_CLIENT --query "SYSTEM START REPLICATED SENDS concurrent_alter_mt_$REPLICA"
        sleep 0.$RANDOM
    done
}


echo "Starting alters"

export -f correct_alter_thread;
export -f insert_thread;
export -f stop_sends_thread;

TIMEOUT=30

timeout $TIMEOUT bash -c stop_sends_thread 2> /dev/null &

timeout $TIMEOUT bash -c correct_alter_thread 2> /dev/null &

timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &

wait

echo "Finishing alters"

for i in `seq $REPLICAS`; do
    $CLICKHOUSE_CLIENT --query "SYSTEM START REPLICATED SENDS concurrent_alter_mt_$i"
done

# This alter will wait for all previous mutations
$CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_alter_mt_1 UPDATE value1 = value1 + 1 WHERE 1 SETTINGS mutations_sync = 2"

# maybe it's redundant
for i in `seq $REPLICAS`; do
    $CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA concurrent_alter_mt_$i"
done


for i in `seq $REPLICAS`; do
    $CLICKHOUSE_CLIENT --query "SELECT SUM(toUInt64(value1)) > $INITIAL_SUM FROM concurrent_alter_mt_$i"
    $CLICKHOUSE_CLIENT --query "SELECT COUNT() FROM system.mutations WHERE is_done=0" # all mutations have to be done
    $CLICKHOUSE_CLIENT --query "SELECT * FROM system.mutations WHERE is_done=0" # for verbose output
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS concurrent_alter_mt_$i"
done
