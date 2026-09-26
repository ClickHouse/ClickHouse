#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-parallel
# no-parallel: the `database_replicated_pause_after_reading_log_pointer` failpoint
#   is `PAUSEABLE_ONCE` and fires globally; a concurrent backup from another test
#   could steal the pause from this test's submitted backup, causing
#   `SYSTEM WAIT FAILPOINT ... PAUSE` to hang or to be unblocked by the wrong query.

# Regression test: backup of a `DatabaseReplicated` must not cause a logical-error
# exception when the database is dropped and recreated (resetting `max_log_ptr`)
# concurrently with the backup operation.
#
# Before the fix, `getConsistentMetadataSnapshotImpl` had
# `chassert(max_log_ptr == new_max_log_ptr)` that fired when the log pointer went
# backwards after database recreation. In release builds the chassert was a no-op
# and the retry loop instead exhausted, throwing `Cannot get consistent metadata
# snapshot`. After the fix, such backups fail cleanly with
# `CANNOT_GET_REPLICATED_DATABASE_SNAPSHOT` and the message
# `Replicated database was dropped`.
#
# This test triggers the race deterministically with the
# `database_replicated_pause_after_reading_log_pointer` failpoint, which pauses
# `getTablesForBackup` after it has read `snapshot_version` (and its `czxid`) from
# the old database but before it calls `getConsistentMetadataSnapshotImpl`. While
# paused, the test does `DROP DATABASE` + `CREATE DATABASE` at the same Keeper path
# so the new database gets a fresh `czxid` and starts with `max_log_ptr = 1`, then
# notifies the failpoint. Because `getTablesForBackup` forwards the captured `czxid`
# as `expected_max_log_ptr_czxid`, the recreate is caught by the entry-time `czxid`
# identity check at the top of `getConsistentMetadataSnapshotImpl` (not by the
# in-loop rollback guard, which is covered by
# `04320_backup_replicated_db_recreate_rollback_guard.sh`), throwing
# `Replicated database was dropped`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="db_$CLICKHOUSE_DATABASE"
ZK_PATH="/clickhouse/databases/$DB"
BACKUP_ID="${CLICKHOUSE_DATABASE}_recreate_test"
FAILPOINT="database_replicated_pause_after_reading_log_pointer"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" > /dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT --query "SYSTEM UNFREEZE WITH ID = '$BACKUP_ID'" > /dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS $DB SYNC" > /dev/null 2>&1 ||:
}
trap cleanup EXIT
cleanup

# Create the original database with a handful of tables so `max_log_ptr` advances
# above 1 (each `CREATE TABLE` bumps `max_log_ptr`). When we recreate the database
# below, the new instance gets a fresh `czxid` and starts at `max_log_ptr = 1`. The
# changed `czxid` is what the entry-time identity check detects; the smaller value is
# incidental here (the dedicated rollback-guard coverage lives in `04320`).
$CLICKHOUSE_CLIENT --query "
    CREATE DATABASE $DB ENGINE = Replicated('$ZK_PATH', 's0', 'r0');
" > /dev/null
for i in $(seq 1 10); do
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE $DB.t_$i (x UInt64) ENGINE = MergeTree ORDER BY x;
    " > /dev/null
done

# Arm the failpoint and submit the backup. `BACKUP ... ASYNC` returns immediately;
# the actual snapshot work runs on a server background thread, which is the thread
# that hits the failpoint inside `getTablesForBackup`.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FAILPOINT"
$CLICKHOUSE_CLIENT --query "
    BACKUP DATABASE $DB TO Disk('backups', '$BACKUP_ID')
    SETTINGS id = '$BACKUP_ID' ASYNC
" > /dev/null

# Block until the backup thread is paused at the failpoint (i.e. has finished
# reading `snapshot_version` but has not yet entered the snapshot loop).
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"

# Drop and recreate at the same Keeper path. This removes `/max_log_ptr` and
# recreates it with a new `czxid` and value `1`. The paused backup still holds
# a `DatabasePtr` to the old in-memory `DatabaseReplicated`, so when it resumes
# it will read the new database's metadata via the old object's `zookeeper_path`.
$CLICKHOUSE_CLIENT --query "DROP DATABASE $DB SYNC"
$CLICKHOUSE_CLIENT --query "
    CREATE DATABASE $DB ENGINE = Replicated('$ZK_PATH', 's0', 'r0');
" > /dev/null

# Resume the backup. It enters `getConsistentMetadataSnapshotImpl` with the `czxid`
# captured before the recreate forwarded as `expected_max_log_ptr_czxid`. The
# function's entry-time `tryGet` of `/max_log_ptr` now observes the recreated node's
# new `czxid`, which differs from the expected one, so the entry-time identity check
# throws `Replicated database was dropped` before the snapshot read or the in-loop
# rollback guard is reached.
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FAILPOINT"

# Wait for the backup to leave `CREATING_BACKUP`. The failpoint fires once and
# the snapshot path either succeeds or fails immediately, so this drain should
# complete in well under a second; the deadline is just a safety bound.
DEADLINE=$((SECONDS + 30))
while [[ $SECONDS -lt $DEADLINE ]]; do
    STATUS=$($CLICKHOUSE_CLIENT --query "
        SELECT status FROM system.backups WHERE id = '$BACKUP_ID' LIMIT 1
    ")
    [[ "$STATUS" != "CREATING_BACKUP" ]] && break
    sleep 0.1
done

# Bugfix-validation signal #1 (regression detection): the pre-fix code paths must
# not fire. Pre-fix in debug builds the `chassert(max_log_ptr == new_max_log_ptr)`
# fires and surfaces as a `LOGICAL_ERROR` exception in the backup error; in
# release builds the chassert is a no-op and the retry loop exhausts, throwing
# `Cannot get consistent metadata snapshot`. The fix replaces both with the
# `Replicated database was dropped` message, so neither pre-fix symptom can
# appear here.
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.backups
    WHERE id = '$BACKUP_ID'
      AND (error LIKE '%LOGICAL_ERROR%'
           OR error LIKE '%Cannot get consistent metadata snapshot%')
"

# Bugfix-validation signal #2 (fix engagement): with the failpoint pause we
# guarantee the backup observes the recreate, so the new error must fire exactly
# once. This message does not exist in unfixed code.
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0 FROM system.backups
    WHERE id = '$BACKUP_ID' AND error LIKE '%Replicated database was dropped%'
"

# Liveness check: the server is still alive.
$CLICKHOUSE_CLIENT --query "SELECT 1"
