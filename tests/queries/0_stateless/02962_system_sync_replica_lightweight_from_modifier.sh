#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest, no-shared-merge-tree

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./replication.lib
. "$CURDIR"/replication.lib

TOTAL_REPLICAS=10
REPLICAS_TO_DROP=7
export TOTAL_REPLICAS
export REPLICAS_TO_DROP

for i in $(seq $TOTAL_REPLICAS); do
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_table_$i"
    $CLICKHOUSE_CLIENT --query "CREATE TABLE test_table_$i (key UInt64, value UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_table', '$i') ORDER BY key SETTINGS old_parts_lifetime=1"
done

function insert_thread() {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        REPLICA=$(($RANDOM % $TOTAL_REPLICAS + 1))
        with_lock test_table_$REPLICA $CLICKHOUSE_CLIENT --query "INSERT INTO test_table_$REPLICA VALUES ($RANDOM, $RANDOM % 255)"
        sleep 0.$RANDOM
    done
}

function sync_and_drop_replicas() {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        for i in $(seq $REPLICAS_TO_DROP); do
            local stable_replica_id=$((i + 1))
            # Note that "ALTER TABLE test_table_$i MODIFY SETTING parts_to_throw_insert = 0" does not prevent INSERTs that were run before the setting change
            # We use locks to make sure SYNC/DROP and INSERT are not running concurrently
            with_lock test_table_$i $CLICKHOUSE_CLIENT --query "
                SYSTEM SYNC REPLICA test_table_$stable_replica_id LIGHTWEIGHT FROM '$i';
                DROP TABLE IF EXISTS test_table_$i;
            "
        done

        for i in $(seq $REPLICAS_TO_DROP); do
            $CLICKHOUSE_CLIENT --query "CREATE TABLE test_table_$i (key UInt64, value UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_table', '$i') ORDER BY key SETTINGS old_parts_lifetime=1"
        done
    done
}

function optimize_thread() {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        REPLICA=$(($RANDOM % $TOTAL_REPLICAS + 1))
        $CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE test_table_$REPLICA FINAL"
        sleep 0.$RANDOM
    done
}

function mutations_thread() {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        REPLICA=$(($RANDOM % $TOTAL_REPLICAS + 1))
        CONDITION="key % 2 = 0"
        $CLICKHOUSE_CLIENT --query "ALTER TABLE test_table_$REPLICA DELETE WHERE $CONDITION"
        sleep 0.$RANDOM
    done
}

function consistency_table_sync_non_existent_replica() {
    echo "Testing sync from non-existent replica..."
    local NON_EXISTENT_REPLICA="non_existent_replica_$RANDOM"

    SYNC_OUTPUT=$($CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA test_table_1 LIGHTWEIGHT FROM '$NON_EXISTENT_REPLICA'" 2>&1)
    EXIT_CODE=$?

    if ! echo "$SYNC_OUTPUT" | grep -q "failed: replica does not exist"; then
        echo "FAILED: Error was not detected properly for non-existent replica"
        echo
        echo "Full output from SYSTEM SYNC command (exit code $EXIT_CODE):"
        echo
        echo "$SYNC_OUTPUT"
        exit 1
    fi

    echo "Error correctly detected for non-existent replica"
}

TIMEOUT=30

consistency_table_sync_non_existent_replica
insert_thread 2> /dev/null &
sync_and_drop_replicas 2> /dev/null &
optimize_thread 2> /dev/null &
mutations_thread 2> /dev/null &

wait

check_replication_consistency "test_table_" "count(), sum(key), sum(value)"

echo "Test completed"

lost_parts_count=$($CLICKHOUSE_CLIENT --query "SELECT SUM(lost_part_count) FROM system.replicas WHERE database=currentDatabase()")
if [ "$lost_parts_count" -ne 0 ]; then
    echo "Data consistency check failed: lost parts count is not zero"
    exit 1
fi

echo "Data consistency check passed"

for i in $(seq $TOTAL_REPLICAS); do
    if [ $i -gt $REPLICAS_TO_DROP ]; then
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_table_$i"
    fi
done
