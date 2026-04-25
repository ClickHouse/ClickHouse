#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest

# Regression test: backup of a DatabaseReplicated must not cause a logical error
# exception when the database is dropped and recreated (resetting max_log_ptr)
# concurrently with the backup operation.
#
# Before the fix, `getConsistentMetadataSnapshotImpl` had
# `chassert(max_log_ptr == new_max_log_ptr)` that fired when the log pointer
# went backwards after database recreation. After the fix, such backups fail
# cleanly with `CANNOT_GET_REPLICATED_DATABASE_SNAPSHOT`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="db_$CLICKHOUSE_DATABASE"
ZK_PATH="/clickhouse/databases/$DB"

TIMEOUT=10

RECREATE_COUNT_FILE=$(mktemp)
trap 'rm -f "$RECREATE_COUNT_FILE"' EXIT

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
    local count=0
    while true; do
        [[ $SECONDS -gt $TIMEOUT ]] && break
        if $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS $DB SYNC" > /dev/null 2>&1; then
            count=$((count + 1))
        fi
        create_and_populate
    done
    echo "$count" > "$RECREATE_COUNT_FILE"
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

# Verify the test actually exercised the regression path: at least one full
# DROP+recreate cycle must have completed within $TIMEOUT seconds, otherwise
# `max_log_ptr` never moved backwards and the regression is untested.
RECREATE_COUNT=$(cat "$RECREATE_COUNT_FILE")
if [[ $RECREATE_COUNT -ge 1 ]]; then
    echo "recreated"
else
    echo "not recreated: $RECREATE_COUNT"
fi

# Detect the original regression: the LOGICAL_ERROR exception. Backups may
# legitimately fail under concurrent DROP, but they must never fail with a
# LOGICAL_ERROR. This is what the chassert previously caught.
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.backups
    WHERE id LIKE '${CLICKHOUSE_DATABASE}_recreate_%'
      AND error LIKE '%LOGICAL_ERROR%'
"

# Clean up backup state.
$CLICKHOUSE_CLIENT --query "
    SELECT id FROM system.backups
    WHERE id LIKE '${CLICKHOUSE_DATABASE}_recreate_%'
" | while read -r backup_id; do
    $CLICKHOUSE_CLIENT --query "SYSTEM UNFREEZE WITH ID = '$backup_id'" > /dev/null 2>&1
done

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS $DB SYNC" > /dev/null 2>&1

# Liveness check: the server is still alive.
$CLICKHOUSE_CLIENT --query "SELECT 1"
