#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database

# Race between DROP vs UNDROP

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_PREFIX="test_race_drop_undrop_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    for i in {1..4}; do
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${TABLE_PREFIX}_$i SYNC" 2>/dev/null ||:
    done
}

trap cleanup EXIT
cleanup

# Run multiple iterations to increase chance of hitting the race
for iter in {1..3}; do
    # Create tables using Memory engine - tables without disk storage have
    # ignore_delay=true, making them immediately eligible for background drop
    for i in {1..4}; do
        $CLICKHOUSE_CLIENT --query "CREATE TABLE IF NOT EXISTS ${TABLE_PREFIX}_$i (x Int32) ENGINE = Memory" 2>/dev/null ||:
        $CLICKHOUSE_CLIENT --query "INSERT INTO ${TABLE_PREFIX}_$i VALUES ($i)" 2>/dev/null ||:
    done

    # Drop tables asynchronously to populate the drop queue
    # This queues entries into tables_marked_dropped list and schedules
    # the background dropTableDataTask immediately (ignore_delay=true)
    for i in {1..4}; do
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${TABLE_PREFIX}_$i" \
            --database_atomic_wait_for_drop_and_detach_synchronously=0 2>/dev/null &
    done

    # Concurrently try to undrop tables - this races with dropTableDataTask
    # undropTable() erases from tables_marked_dropped while dropTablesParallel
    # is dereferencing iterators without holding the lock
    for i in {1..4}; do
        $CLICKHOUSE_CLIENT --query "UNDROP TABLE ${TABLE_PREFIX}_$i" 2>/dev/null &
    done
    wait

    # Clean up for next iteration - drop all tables that might exist
    for i in {1..4}; do
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${TABLE_PREFIX}_$i SYNC" 2>/dev/null ||:
    done
done
