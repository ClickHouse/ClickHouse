#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-random-settings

# Regression test: backup of a DatabaseReplicated must not cause a logical error
# exception when the database is dropped and recreated (resetting max_log_ptr)
# concurrently with the backup operation.
#
# Before the fix, `getConsistentMetadataSnapshotImpl` had
# `chassert(max_log_ptr == new_max_log_ptr)` that fired when the log pointer
# went backwards after database recreation. After the fix, such backups fail
# cleanly with `CANNOT_GET_REPLICATED_DATABASE_SNAPSHOT`.
#
# `no-random-settings` is required because settings like `fsync_metadata=1` and
# `max_threads=1` make `CREATE DATABASE Replicated` and `DROP DATABASE SYNC`
# slow enough to push the stress loop past the 180s test budget under
# sanitizer builds, while none of the randomized settings affect what this
# test is checking.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="db_$CLICKHOUSE_DATABASE"
ZK_PATH="/clickhouse/databases/$DB"

TIMEOUT=45

# A non-trivial number of tables widens the race window inside
# `getConsistentMetadataSnapshotImpl`: every iteration of its retry loop calls
# `tryGet` over one Keeper path per table, so more tables means a longer
# wall-clock iteration during which a concurrent `DROP`+recreate can land
# between reading `snapshot_version` and reading `max_log_ptr`. With a single
# table the iteration completes in a few milliseconds, which made the
# pre-fix `Log pointer moved backwards` symptom miss most of the time in
# bugfix validation.
NUM_TABLES=20

function create_and_populate()
{
    $CLICKHOUSE_CLIENT --query "
        CREATE DATABASE IF NOT EXISTS $DB ENGINE = Replicated('$ZK_PATH', 's0', 'r0');
    " > /dev/null 2>&1
    local create_tables=""
    for i in $(seq 1 $NUM_TABLES); do
        create_tables+="CREATE TABLE IF NOT EXISTS $DB.t_$i (x UInt64) ENGINE = MergeTree ORDER BY x;"
    done
    $CLICKHOUSE_CLIENT --query "$create_tables" > /dev/null 2>&1
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
# requests in a tight loop; the backups run concurrently on the server. More
# workers widen the chance of catching the narrow race between reading
# `snapshot_version` and entering the snapshot loop in
# `getConsistentMetadataSnapshotImpl`.
do_backups &
do_backups &
do_backups &
do_backups &
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
# the racy code path after the loops above exit. Wait for submitted backups to
# leave `CREATING_BACKUP` before checking error states. The race window is at
# the very start of the backup (the `getConsistentMetadataSnapshotImpl` loop),
# so a backup that is still running after this drain has already passed the
# racy phase and cannot be hiding a delayed `LOGICAL_ERROR`. The bound exists
# only to keep the test inside the 180s budget on slow sanitizer builds.
DEADLINE=$((SECONDS + 30))
while [[ $SECONDS -lt $DEADLINE ]]; do
    IN_PROGRESS=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.backups
        WHERE id LIKE '${CLICKHOUSE_DATABASE}_recreate_%' AND status = 'CREATING_BACKUP'
    ")
    [[ "$IN_PROGRESS" == "0" ]] && break
    sleep 0.5
done

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

# Clean up backup state. The stress loop submits thousands of backup IDs, and
# unfreezing each one is a sequential round trip to the server. Bound the
# cleanup so a slow run does not push the test past the 180s budget; any
# leftover frozen hardlinks are removed by the test infrastructure when the
# database directory is wiped between runs.
#
# Read all IDs into an array first instead of streaming through a pipe: when
# the deadline triggers `break` mid-iteration, a piped `clickhouse-client`
# would still be writing and would surface a `Broken pipe` error on stderr,
# failing the test on the harness's "having stderror" check.
CLEANUP_DEADLINE=$((SECONDS + 20))
mapfile -t BACKUP_IDS < <($CLICKHOUSE_CLIENT --query "
    SELECT id FROM system.backups
    WHERE id LIKE '${CLICKHOUSE_DATABASE}_recreate_%'
")
for backup_id in "${BACKUP_IDS[@]}"; do
    [[ $SECONDS -gt $CLEANUP_DEADLINE ]] && break
    $CLICKHOUSE_CLIENT --query "SYSTEM UNFREEZE WITH ID = '$backup_id'" > /dev/null 2>&1
done

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS $DB SYNC" > /dev/null 2>&1

# Liveness check: the server is still alive.
$CLICKHOUSE_CLIENT --query "SELECT 1"
