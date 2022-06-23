#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS concurrent_mutate_kill"

$CLICKHOUSE_CLIENT --query "CREATE TABLE concurrent_mutate_kill (key UInt64, value String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/concurrent_mutate_kill', '1') ORDER BY key PARTITION BY key % 100 SETTINGS max_replicated_mutations_in_queue=1000, number_of_free_entries_in_pool_to_execute_mutation=0,max_replicated_merges_in_queue=1000"

$CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_mutate_kill SELECT number, toString(number) FROM numbers(1000000)"

function alter_thread
{
    while true; do
        TYPE=$($CLICKHOUSE_CLIENT --query "SELECT type FROM system.columns WHERE table='concurrent_mutate_kill' and database='${CLICKHOUSE_DATABASE}' and name='value'")
        if [ "$TYPE" == "String" ]; then
            $CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_mutate_kill MODIFY COLUMN value UInt64 SETTINGS replication_alter_partitions_sync=2"
        else
            $CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_mutate_kill MODIFY COLUMN value String SETTINGS replication_alter_partitions_sync=2"
        fi
    done
}

function kill_mutation_thread
{
    while true; do
        # find any mutation and kill it
        mutation_id=$($CLICKHOUSE_CLIENT --query "SELECT mutation_id FROM system.mutations WHERE is_done=0 and database='${CLICKHOUSE_DATABASE}' and table='concurrent_mutate_kill' LIMIT 1")
        if [ ! -z "$mutation_id" ]; then
            $CLICKHOUSE_CLIENT --query "KILL MUTATION WHERE mutation_id='$mutation_id' and table='concurrent_mutate_kill' and database='${CLICKHOUSE_DATABASE}'" 1> /dev/null
            sleep 1
        fi
    done
}


export -f alter_thread;
export -f kill_mutation_thread;

TIMEOUT=30

timeout $TIMEOUT bash -c alter_thread 2> /dev/null &
timeout $TIMEOUT bash -c kill_mutation_thread 2> /dev/null &

wait

$CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA concurrent_mutate_kill"

# with timeout alter query can be not finished yet, so to execute new alter
# we use retries
counter=0
while true; do
    if $CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_mutate_kill MODIFY COLUMN value Int64 SETTINGS replication_alter_partitions_sync=2" 2> /dev/null ; then
        break
    fi

    if [ "$counter" -gt 120 ]
    then
        break
    fi
    sleep 0.5
    counter=$(($counter + 1))
done

$CLICKHOUSE_CLIENT --query "SHOW CREATE TABLE concurrent_mutate_kill"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE concurrent_mutate_kill FINAL"
$CLICKHOUSE_CLIENT --query "SELECT sum(value) FROM concurrent_mutate_kill"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS concurrent_mutate_kill"
