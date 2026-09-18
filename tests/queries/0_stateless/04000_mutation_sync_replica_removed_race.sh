#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/97309
#
# When waitMutationToFinishOnReplicas waited for a replica that got removed
# (its mutation_pointer ZK node disappeared), the code broke out of the wait
# loop without adding the replica to inactive_replicas. This caused a
# chassert(mutation_status->is_done) to fire.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_PREFIX="mutation_sync_race"
TABLE_R1="${TABLE_PREFIX}_r1"
TABLE_R2="${TABLE_PREFIX}_r2"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${TABLE_R1} SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${TABLE_R2} SYNC"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE ${TABLE_R1} (key UInt64, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${TABLE_PREFIX}', '1')
    ORDER BY key
"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE ${TABLE_R2} (key UInt64, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${TABLE_PREFIX}', '2')
    ORDER BY key
"

$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 --query "
    INSERT INTO ${TABLE_R1} SELECT number, toString(number) FROM numbers(1000)
"

$CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA ${TABLE_R2}"

# Thread 1: run synchronous mutations in a loop.
# mutations_sync=2 triggers waitMutationToFinishOnReplicas which waits for both replicas.
# When the second replica is dropped mid-wait, this used to hit chassert(mutation_status->is_done).
# Errors are expected (UNFINISHED, TABLE_IS_DROPPED, etc.) and suppressed — we only care
# that the server doesn't crash.
function mutation_thread()
{
    local TIMELIMIT=$((SECONDS + TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT --query "
            ALTER TABLE ${TABLE_R1} UPDATE value = toString(toUInt64(value) + 1) WHERE 1
            SETTINGS mutations_sync = 2
        " >/dev/null 2>&1 ||:
    done
}

# Thread 2: repeatedly drop and recreate the second replica.
# This removes its mutation_pointer ZK node, triggering the race in waitMutationToFinishOnReplicas.
function drop_replica_thread()
{
    local TIMELIMIT=$((SECONDS + TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${TABLE_R2} SYNC" >/dev/null 2>&1 ||:
        sleep 0.$((RANDOM % 3))
        $CLICKHOUSE_CLIENT --query "
            CREATE TABLE ${TABLE_R2} (key UInt64, value String)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${TABLE_PREFIX}', '2')
            ORDER BY key
        " >/dev/null 2>&1 ||:
        sleep 0.$((RANDOM % 3))
    done
}

TIMEOUT=15

mutation_thread &
drop_replica_thread &

wait

# Cleanup: ensure replica 2 exists for final sync
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${TABLE_R2} SYNC" 2>/dev/null
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE ${TABLE_R2} (key UInt64, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${TABLE_PREFIX}', '2')
    ORDER BY key
" 2>/dev/null ||:

$CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA ${TABLE_R1}" 2>/dev/null ||:
$CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA ${TABLE_R2}" 2>/dev/null ||:

# Verify the server is alive and data is readable.
$CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM ${TABLE_R1}"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${TABLE_R1} SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${TABLE_R2} SYNC"
