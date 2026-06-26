#!/usr/bin/env bash
# Tags: race, zookeeper, no-replicated-database, no-shared-merge-tree

# Regression test for phantom entries in mutations' parts_to_do.
#
# When PartCheckThread re-enqueues a GET_PART for a part that has already
# been mutated, addPartToMutations re-adds the old part to parts_to_do.
# On successful (skipped) completion, removePartsCoveredBy does not remove
# the part itself, leaving the mutation stuck forever.
#
# This test runs concurrent inserts, mutations, and part corruption+checks
# in a loop to increase the chance of hitting the race condition.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./replication.lib
. "$CURDIR"/replication.lib

set -e

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS phantom_r1;
    DROP TABLE IF EXISTS phantom_r2;

    CREATE TABLE phantom_r1 (key UInt64, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/phantom', 'r1')
    ORDER BY key
    SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 0,
    cleanup_thread_preferred_points_per_iteration = 0;

    CREATE TABLE phantom_r2 (key UInt64, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/phantom', 'r2')
    ORDER BY key
    SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 0,
    cleanup_thread_preferred_points_per_iteration = 0;

    SYSTEM SYNC REPLICA phantom_r1;
    SYSTEM SYNC REPLICA phantom_r2;
"

function insert_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    local i=0
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "INSERT INTO phantom_r1 VALUES ($((i * 100)), 'data')" 2>/dev/null ||:
        i=$((i+1))
    done
}

function mutation_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "ALTER TABLE phantom_r1 DELETE WHERE rand() % 10 = 0" 2>/dev/null ||:
        sleep 0.1
    done
}

function corrupt_and_check_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        # Get a random active part path on r2
        local part_path
        part_path=$($CLICKHOUSE_CLIENT -q "
            SELECT path FROM system.parts
            WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'phantom_r2' AND active
            ORDER BY rand() LIMIT 1
        " 2>/dev/null) ||:

        if [ -n "$part_path" ]; then
            # Corrupt the part by removing a column data file
            rm -f "${part_path}data.bin" 2>/dev/null ||:
            rm -f "${part_path}data.cmrk3" 2>/dev/null ||:
        fi

        # Trigger part check which will detect corruption and create GET_PART
        $CLICKHOUSE_CLIENT -q "CHECK TABLE phantom_r2" 1>/dev/null 2>/dev/null ||:

        sleep 0.2
    done
}

TIMEOUT=15

insert_thread &
mutation_thread &
corrupt_and_check_thread &

wait

check_replication_consistency "phantom_r" "count(), sum(key)"

$CLICKHOUSE_CLIENT -q "DROP TABLE phantom_r1" 2> >(grep -F -v 'is already started to be removing by another replica right now') &
$CLICKHOUSE_CLIENT -q "DROP TABLE phantom_r2" 2> >(grep -F -v 'is already started to be removing by another replica right now') &
wait
