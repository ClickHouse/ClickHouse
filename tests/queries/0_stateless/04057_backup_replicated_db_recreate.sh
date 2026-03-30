#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest

# Regression test: backup of a DatabaseReplicated must not cause a logical error
# exception when the database is dropped and recreated (resetting max_log_ptr)
# concurrently with the backup operation.
#
# Before the fix, `getConsistentMetadataSnapshotImpl` had
# `chassert(max_log_ptr == new_max_log_ptr)` that fired when the log pointer
# went backwards after database recreation.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="db_$CLICKHOUSE_DATABASE"
ZK_PATH="/clickhouse/databases/$DB"

TIMEOUT=10

function create_and_populate()
{
    $CLICKHOUSE_CLIENT --query "
        CREATE DATABASE IF NOT EXISTS $DB ENGINE = Replicated('$ZK_PATH', 's0', 'r0');
    " > /dev/null 2>&1

    # Create several tables to advance max_log_ptr.
    for i in $(seq 1 5); do
        $CLICKHOUSE_CLIENT --query "
            CREATE TABLE IF NOT EXISTS $DB.t_$i (x UInt64) ENGINE = MergeTree ORDER BY x;
        " > /dev/null 2>&1
    done
}

function drop_and_recreate()
{
    while true; do
        [[ $SECONDS -gt $TIMEOUT ]] && break
        $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS $DB SYNC" > /dev/null 2>&1
        create_and_populate
    done
}

function do_backups()
{
    local WORKER_ID=$BASHPID
    local I=0
    while true; do
        [[ $SECONDS -gt $TIMEOUT ]] && break
        I=$((I + 1))
        $CLICKHOUSE_CLIENT --query "
            BACKUP DATABASE $DB TO Disk('backups', '${CLICKHOUSE_DATABASE}_recreate_${WORKER_ID}_$I')
            SETTINGS id = '${CLICKHOUSE_DATABASE}_recreate_${WORKER_ID}_$I' ASYNC
        " > /dev/null 2>&1
    done
}

# Set up the initial database.
create_and_populate

# Run concurrent backups and database recreation.
drop_and_recreate &
do_backups &
do_backups &

wait

# Check how many backups were attempted (informational; under sanitizers all may
# fail due to the concurrent DROP, but the real assertion is server liveness).
BACKUP_COUNT=$($CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.backups
    WHERE id LIKE '${CLICKHOUSE_DATABASE}_recreate_%'
")
echo "Backups attempted: $BACKUP_COUNT" >&2

# Clean up backup state.
$CLICKHOUSE_CLIENT --query "
    SELECT id FROM system.backups
    WHERE id LIKE '${CLICKHOUSE_DATABASE}_recreate_%'
" | while read -r backup_id; do
    $CLICKHOUSE_CLIENT --query "SYSTEM UNFREEZE WITH ID = '$backup_id'" > /dev/null 2>&1
done

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS $DB SYNC" > /dev/null 2>&1

# The main assertion: the server is alive and no logical errors occurred.
$CLICKHOUSE_CLIENT --query "SELECT 1"
