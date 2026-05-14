#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, long, race, no-ordinary-database, no-replicated-database

# Race condition test: concurrent UNDROP vs background dropTableDataTask
# Bug: getTablesToDrop() collects iterators while holding lock, but returns
# them AFTER releasing lock. dropTablesParallel() dereferences these iterators
# without holding any lock. Concurrent undropTable() can erase elements from
# tables_marked_dropped, invalidating those iterators - use-after-free.
# Under TSan, this should detect the race condition.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

TABLE_PREFIX="test_race_drop_undrop_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    for i in {1..10}; do
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${TABLE_PREFIX}_$i SYNC" 2>/dev/null ||:
    done
}

trap cleanup EXIT
cleanup

# Run multiple iterations to increase chance of hitting the race
for iter in {1..3}; do
    # Create tables
    for i in {1..10}; do
        $CLICKHOUSE_CLIENT --query "CREATE TABLE ${TABLE_PREFIX}_$i (x Int32) ENGINE = MergeTree() ORDER BY x"
        $CLICKHOUSE_CLIENT --query "INSERT INTO ${TABLE_PREFIX}_$i VALUES ($i)"
    done

    # Drop tables asynchronously to populate the drop queue
    # This queues entries into tables_marked_dropped list
    for i in {1..10}; do
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${TABLE_PREFIX}_$i" \
            --database_atomic_wait_for_drop_and_detach_synchronously=0 &
    done
    wait
    
    # Trigger the background drop task to start processing
    # This should call getTablesToDrop() and start dropTablesParallel()
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS" 2>/dev/null ||:

    # Concurrently try to undrop tables - this races with dropTableDataTask
    # undropTable() erases from tables_marked_dropped while dropTablesParallel
    # is dereferencing iterators without holding the lock
    for i in {1..10}; do
        $CLICKHOUSE_CLIENT --query "UNDROP TABLE ${TABLE_PREFIX}_$i" 2>/dev/null &
    done
    wait

    # Clean up for next iteration - drop all tables that might exist
    for i in {1..10}; do
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${TABLE_PREFIX}_$i SYNC" 2>/dev/null ||:
    done
done

echo "OK"
