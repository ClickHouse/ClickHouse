#!/usr/bin/env bash
# Tags: replica, no-parallel, no-fasttest


# This test checks mutations concurrent execution with concurrent inserts.
# There was a bug in mutations finalization, when mutation finishes not after all
# MUTATE_PART tasks execution, but after GET of already mutated part from other replica.
# To test it we stop some replicas to delay fetch of required parts for mutation.
# Since our replication queue executing tasks concurrently it may happen, that we dowload already mutated
# part before source part.


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

REPLICAS=5

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS concurrent_mutate_mt_$i"
done

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "CREATE TABLE concurrent_mutate_mt_$i (key UInt64, value1 UInt64, value2 String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/concurrent_mutate_mt', '$i') ORDER BY key SETTINGS max_replicated_mutations_in_queue=1000, number_of_free_entries_in_pool_to_execute_mutation=0,max_replicated_merges_in_queue=1000,temporary_directories_lifetime=10,cleanup_delay_period=3,cleanup_delay_period_random_add=0"
done

$CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_mutate_mt_1 SELECT number, number + 10, toString(number) from numbers(10)"
$CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_mutate_mt_1 SELECT number, number + 10, toString(number) from numbers(10, 40)"

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA concurrent_mutate_mt_$i"
done

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "SELECT SUM(value1) FROM concurrent_mutate_mt_$i"
done

INITIAL_SUM=$($CLICKHOUSE_CLIENT --query "SELECT SUM(value1) FROM concurrent_mutate_mt_1")

# Run mutation on random replica
function correct_alter_thread()
{
    while true; do
        REPLICA=$(($RANDOM % 5 + 1))
        $CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_mutate_mt_$REPLICA UPDATE value1 = value1 + 1 WHERE 1";
        sleep 1
    done
}

# This thread add some data to table.
function insert_thread()
{

    VALUES=(7 8 9)
    while true; do
        REPLICA=$(($RANDOM % 5 + 1))
        VALUE=${VALUES[$RANDOM % ${#VALUES[@]} ]}
        $CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_mutate_mt_$REPLICA VALUES($RANDOM, $VALUE, toString($VALUE))"
        sleep 0.$RANDOM
    done
}

function detach_attach_thread()
{
    while true; do
        REPLICA=$(($RANDOM % 5 + 1))
        $CLICKHOUSE_CLIENT --query "DETACH TABLE concurrent_mutate_mt_$REPLICA"
        sleep 0.$RANDOM
        sleep 0.$RANDOM
        sleep 0.$RANDOM
        $CLICKHOUSE_CLIENT --query "ATTACH TABLE concurrent_mutate_mt_$REPLICA"
    done
}


echo "Starting alters"

export -f correct_alter_thread;
export -f insert_thread;
export -f detach_attach_thread;

# We assign a lot of mutations so timeout shouldn't be too big
TIMEOUT=15

timeout $TIMEOUT bash -c detach_attach_thread 2> /dev/null &

timeout $TIMEOUT bash -c correct_alter_thread 2> /dev/null &

timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &

wait

echo "Finishing alters"

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "ATTACH TABLE concurrent_mutate_mt_$i" 2> /dev/null
done

sleep 1

counter=0
have_undone_mutations_query="select * from system.mutations where table like 'concurrent_mutate_mt_%' and is_done=0 and database='${CLICKHOUSE_DATABASE}'"
have_all_tables_query="select count() FROM system.tables WHERE name LIKE 'concurrent_mutate_mt_%' and database='${CLICKHOUSE_DATABASE}'"

while true ; do
    if [ "$counter" -gt 120 ]
    then
        break
    fi
    sleep 1
    for i in $(seq $REPLICAS); do
        $CLICKHOUSE_CLIENT --query "ATTACH TABLE concurrent_mutate_mt_$i" 2> /dev/null
    done

    counter=$(($counter + 1))

    # no active mutations and all tables attached
    if [[ -z $($CLICKHOUSE_CLIENT --query "$have_undone_mutations_query" 2>&1) && $($CLICKHOUSE_CLIENT --query "$have_all_tables_query" 2>&1) == "$REPLICAS" ]]; then
        break
    fi
done

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "SELECT SUM(toUInt64(value1)) > $INITIAL_SUM FROM concurrent_mutate_mt_$i"
    $CLICKHOUSE_CLIENT --query "SELECT COUNT() FROM system.mutations WHERE table='concurrent_mutate_mt_$i' and is_done=0" # all mutations have to be done
    $CLICKHOUSE_CLIENT --query "SELECT * FROM system.mutations WHERE table='concurrent_mutate_mt_$i' and is_done=0" # for verbose output
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS concurrent_mutate_mt_$i"
done
