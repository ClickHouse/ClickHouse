#!/usr/bin/env bash
# Tags: race, long, no-fasttest, no-replicated-database
#   race               -- relies on concurrent system.columns queries racing with DETACH
#   long               -- needs multiple iterations to hit the race window
#   no-fasttest        -- uses Ordinary database engine which requires special settings
#   no-replicated-database -- uses Ordinary database engine

# Regression test for `assertHasValidVersionMetadata` assertion failure.
#
# When `system.columns` captures `shared_ptr<IStorage>` to a MergeTree table
# in an Ordinary database that has parts with non-prehistoric `creation_tid`,
# and the table is DETACH'ed while its part directories are externally removed,
# the storage destructor can fire with parts referencing deleted `txn_version.txt`
# files, causing `chassert(assertHasValidVersionMetadata())` to abort.

# Creation of a database with Ordinary engine emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db_name="${CLICKHOUSE_DATABASE}_ordinary_04039"

$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=1 \
    -q "CREATE DATABASE IF NOT EXISTS ${db_name} ENGINE=Ordinary"

for attempt in $(seq 1 5); do
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${db_name}.test_intersect SYNC"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE ${db_name}.test_intersect (x UInt64) ENGINE = MergeTree ORDER BY x"

    # Insert inside a transaction to create a part with non-prehistoric `creation_tid`.
    # The INSERT succeeds (creates the part) even though the transaction then fails
    # because Ordinary databases do not support transactions.
    $CLICKHOUSE_CLIENT -q "BEGIN TRANSACTION; INSERT INTO ${db_name}.test_intersect SETTINGS async_insert=0 VALUES (1); COMMIT" 2>/dev/null ||:

    # DETACH + ATTACH so the part's version metadata is fixed up on load.
    $CLICKHOUSE_CLIENT -q "DETACH TABLE ${db_name}.test_intersect"
    $CLICKHOUSE_CLIENT -q "ATTACH TABLE ${db_name}.test_intersect"

    # Get data directory path.
    data_dir=$($CLICKHOUSE_CLIENT -q "
        SELECT arrayJoin(data_paths) FROM system.tables
        WHERE database = '${db_name}' AND name = 'test_intersect'" | head -1)

    # Insert a few more parts.
    for i in 2 3 4 5; do
        $CLICKHOUSE_CLIENT -q "INSERT INTO ${db_name}.test_intersect SETTINGS async_insert=0 VALUES ($i)"
    done

    # Launch `system.columns` queries that capture `shared_ptr<IStorage>` to all tables.
    pids=()
    for j in $(seq 1 5); do
        $CLICKHOUSE_CLIENT -q "SELECT * FROM system.columns FORMAT Null" 2>/dev/null &
        pids+=($!)
    done

    # Give background system.columns queries time to start and capture storage references.
    sleep 0.05

    # DETACH while `system.columns` is running -- `ColumnsSource` may hold the last ref.
    $CLICKHOUSE_CLIENT -q "DETACH TABLE ${db_name}.test_intersect"

    # Remove the first part directory from disk (including `txn_version.txt`).
    # This mimics what `03915_intersecting_parts_rollback` does between DETACH/ATTACH.
    if [ -n "$data_dir" ]; then
        for d in "${data_dir}"all_*; do
            [ -d "$d" ] || continue
            rm -rf "$d"
            break
        done
    fi

    # Wait per-PID to suppress bash "Killed" messages on stderr.
    for pid in "${pids[@]}"; do
        wait "$pid" 2>/dev/null
    done

    # Re-ATTACH so the next iteration's DROP can find the table.
    $CLICKHOUSE_CLIENT -q "ATTACH TABLE ${db_name}.test_intersect" 2>/dev/null ||:
done

$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=1 -q "DROP DATABASE IF EXISTS ${db_name}"
