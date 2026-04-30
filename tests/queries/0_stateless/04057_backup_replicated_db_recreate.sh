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

TIMEOUT=30

function create_and_populate()
{
    $CLICKHOUSE_CLIENT --query "
        CREATE DATABASE IF NOT EXISTS $DB ENGINE = Replicated('$ZK_PATH', 's0', 'r0');
    " > /dev/null 2>&1
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE IF NOT EXISTS $DB.t (x UInt64) ENGINE = MergeTree ORDER BY x;
    " > /dev/null 2>&1
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

# Start backup workers in the background. They submit `BACKUP ... ASYNC`
# requests in a tight loop; the backups run concurrently on the server.
do_backups &
do_backups &

# Run database recreation in the foreground synchronously. Foreground execution
# guarantees that recreate cycles actually complete and are not lost to the
# background scheduler under randomized settings.
RECREATE_COUNT=0
while [[ $SECONDS -lt $TIMEOUT ]]; do
    if $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS $DB SYNC" > /dev/null 2>&1; then
        RECREATE_COUNT=$((RECREATE_COUNT + 1))
    fi
    create_and_populate
done

wait

# Verify the test actually exercised the regression path: at least one full
# DROP+recreate cycle must have completed, otherwise `max_log_ptr` never moved
# backwards and the regression is untested.
if [[ $RECREATE_COUNT -ge 1 ]]; then
    echo "recreated"
else
    echo "not recreated: $RECREATE_COUNT"
fi

# `BACKUP ... ASYNC` only waits for submission; in-flight backups may still hit
# the racy code path after the loops above exit. Wait for all submitted backups
# to leave `CREATING_BACKUP` before checking error states, otherwise we may miss
# the `LOGICAL_ERROR` this test is meant to catch.
DEADLINE=$((SECONDS + 60))
IN_PROGRESS=1
while [[ $SECONDS -lt $DEADLINE ]]; do
    IN_PROGRESS=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.backups
        WHERE id LIKE '${CLICKHOUSE_DATABASE}_recreate_%' AND status = 'CREATING_BACKUP'
    ")
    [[ "$IN_PROGRESS" == "0" ]] && break
    sleep 0.5
done

# Fail explicitly if backups are still in flight: otherwise the assertion below
# may run too early and miss a `LOGICAL_ERROR` that surfaces a moment later.
if [[ "$IN_PROGRESS" != "0" ]]; then
    echo "still in progress: $IN_PROGRESS"
fi

# Detect the original regression. Pre-fix, the bug manifests in two ways:
#   * In debug builds, the `chassert(max_log_ptr == new_max_log_ptr)` fires
#     and surfaces as a `LOGICAL_ERROR` exception in the backup error.
#   * In release builds, the chassert is a no-op and the loop silently retries
#     until `max_retries` is exhausted, then throws
#     `Cannot get consistent metadata snapshot`.
# The fix replaces both with a distinct `Log pointer moved backwards`
# exception, so neither pre-fix symptom can appear.
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.backups
    WHERE id LIKE '${CLICKHOUSE_DATABASE}_recreate_%'
      AND (error LIKE '%LOGICAL_ERROR%'
           OR error LIKE '%Cannot get consistent metadata snapshot%')
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
