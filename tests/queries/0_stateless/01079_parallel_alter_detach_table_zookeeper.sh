#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

REPLICAS=3

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS concurrent_alter_detach_$i" 2>&1 | tr '\n' ' ' | grep -Fv "TIMEOUT_EXCEEDED" ||:
done

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "CREATE TABLE concurrent_alter_detach_$i (key UInt64, value1 UInt8, value2 UInt8)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/concurrent_alter_detach', '$i') ORDER BY key
    SETTINGS max_replicated_mutations_in_queue=1000, number_of_free_entries_in_pool_to_execute_mutation=0,max_replicated_merges_in_queue=1000,
    temporary_directories_lifetime=10,cleanup_delay_period=3,cleanup_delay_period_random_add=0,cleanup_thread_preferred_points_per_iteration=0"
done

$CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_alter_detach_1 SELECT number, number + 10, number from numbers(10)"
$CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_alter_detach_1 SELECT number, number + 10, number from numbers(10, 40)"

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
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        REPLICA=$(($RANDOM % 3 + 1))
        TYPE=${TYPES[$RANDOM % ${#TYPES[@]} ]}
        $CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_alter_detach_$REPLICA MODIFY COLUMN value1 $TYPE SETTINGS replication_alter_partitions_sync=0"; # additionaly we don't wait anything for more heavy concurrency
        sleep 0.$RANDOM
    done
}

# This thread add some data to table. After we finish we can check, that
# all our data have same types.
# insert queries will fail sometime because of wrong types.
function insert_thread()
{
    VALUES=(7.0 7 '7')
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        REPLICA=$(($RANDOM % 3 + 1))
        VALUE=${VALUES[$RANDOM % ${#VALUES[@]} ]}
        $CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_alter_detach_$REPLICA VALUES($RANDOM, $VALUE, $VALUE)"
        sleep 0.$RANDOM
    done
}

function detach_attach_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        REPLICA=$(($RANDOM % 3 + 1))
        $CLICKHOUSE_CLIENT --query "DETACH TABLE concurrent_alter_detach_$REPLICA"
        sleep 0.$RANDOM
        $CLICKHOUSE_CLIENT --query "ATTACH TABLE concurrent_alter_detach_$REPLICA"
    done
}

echo "Starting alters"

TIMEOUT=15

# Sometimes we detach and attach tables
detach_attach_thread 2> /dev/null &

correct_alter_thread 2> /dev/null &

insert_thread 2> /dev/null &
insert_thread 2> /dev/null &
insert_thread 2> /dev/null &
insert_thread 2> /dev/null &
insert_thread 2> /dev/null &

wait

echo "Finishing alters"

sleep 1

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "ATTACH TABLE concurrent_alter_detach_$i" 2> /dev/null
done

# This alter will finish all previous, but replica 1 maybe still not up-to-date.
# Limit retries to avoid infinite loops that cause test timeouts (especially in DatabaseReplicated mode
# where DDL operations go through a distributed queue and can take longer).
for _ in {1..5}; do
    output=$(timeout 60 ${CLICKHOUSE_CLIENT} --query "ALTER TABLE concurrent_alter_detach_1 MODIFY COLUMN value1 String SETTINGS replication_alter_partitions_sync=2" 2>&1)
    if [[ -z "$output" ]]; then
        break
    fi
    sleep 1
    # just try to attach table if it failed for some reason in the code above
    for i in $(seq $REPLICAS); do
        $CLICKHOUSE_CLIENT --query "ATTACH TABLE concurrent_alter_detach_$i" 2> /dev/null
    done
done

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --receive_timeout 120 --query "SYSTEM SYNC REPLICA concurrent_alter_detach_$i"
    $CLICKHOUSE_CLIENT --query "SELECT SUM(toUInt64(value1)) > $INITIAL_SUM FROM concurrent_alter_detach_$i"

    # Wait for all mutations and replication queue entries to finish.
    # In this case, it might be more robust than SYSTEM SYNC REPLICA
    # for unclear reasons.
    for _ in {1..120}; do
        mutations_count=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' AND is_done = 0 AND table = 'concurrent_alter_detach_$i'")
        queue_count=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.replication_queue WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'concurrent_alter_detach_$i' AND (type = 'ALTER_METADATA' OR type = 'MUTATE_PART')")
        [[ "$mutations_count" == "0" && "$queue_count" == "0" ]] && break
        sleep 1
    done

    $CLICKHOUSE_CLIENT --query "SELECT COUNT() FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and is_done=0 and table = 'concurrent_alter_detach_$i'" # all mutations have to be done
    $CLICKHOUSE_CLIENT --query "SELECT * FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and is_done=0 and table = 'concurrent_alter_detach_$i'" # all mutations have to be done
    $CLICKHOUSE_CLIENT --query "SELECT * FROM system.replication_queue WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'concurrent_alter_detach_$i' and (type = 'ALTER_METADATA' or type = 'MUTATE_PART')" # all mutations and alters have to be done
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS concurrent_alter_detach_$i" 2>&1 | tr '\n' ' ' | grep -Fv "TIMEOUT_EXCEEDED" ||:
done
