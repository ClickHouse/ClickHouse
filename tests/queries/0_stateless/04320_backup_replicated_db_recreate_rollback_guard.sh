#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-parallel
# no-parallel: the `database_replicated_pause_after_snapshot_identity_check` failpoint
#   is `PAUSEABLE_ONCE` and fires globally; a concurrent snapshot from another test
#   could steal the pause from this test's submitted backup, causing
#   `SYSTEM WAIT FAILPOINT ... PAUSE` to hang or to be unblocked by the wrong query.

# Regression test specifically for the in-function `max_log_ptr > new_max_log_ptr`
# rollback guard inside `getConsistentMetadataSnapshotImpl`.
#
# `04057_backup_replicated_db_recreate.sh` and
# `04309_backup_replicated_db_recreate_advance.sh` both pause at
# `database_replicated_pause_after_reading_log_pointer`, which fires in
# `getTablesForBackup` *before* `getConsistentMetadataSnapshotImpl`. Because
# `getTablesForBackup` forwards `snapshot_version_stat.czxid`, a `DROP`+recreate
# during that pause is caught by the entry-time `czxid` identity check at the top of
# `getConsistentMetadataSnapshotImpl`, never by the in-loop rollback guard. So if the
# `max_log_ptr > new_max_log_ptr` guard regressed, those tests would still pass via
# the `czxid` mismatch.
#
# This test pauses at `database_replicated_pause_after_snapshot_identity_check`, which
# fires *after* the entry-time identity check has already passed with the original
# `czxid` but *before* the metadata / `max_log_ptr` read. The recreated database
# starts with `max_log_ptr = 1`, strictly below the captured `snapshot_version`, so on
# resume the rollback guard fires with `log pointer moved from N to 1`. This makes the
# rollback guard itself the thing under test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="db_$CLICKHOUSE_DATABASE"
ZK_PATH="/clickhouse/databases/$DB"
BACKUP_ID="${CLICKHOUSE_DATABASE}_rollback_guard_test"
FAILPOINT="database_replicated_pause_after_snapshot_identity_check"

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
# below, the new instance starts at `max_log_ptr = 1`, strictly less than the
# `snapshot_version` captured before the recreate. This is the rollback the in-loop
# monotonicity guard catches.
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
# that hits the failpoint inside `getConsistentMetadataSnapshotImpl`.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FAILPOINT"
$CLICKHOUSE_CLIENT --query "
    BACKUP DATABASE $DB TO Disk('backups', '$BACKUP_ID')
    SETTINGS id = '$BACKUP_ID' ASYNC
" > /dev/null

# Block until the backup thread is paused at the failpoint (i.e. has passed the
# entry-time `czxid` identity check but has not yet read the metadata snapshot).
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"

# Drop and recreate at the same Keeper path. This removes `/max_log_ptr` and
# recreates it with a new `czxid` and value `1`. The entry-time identity check has
# already passed with the original `czxid`, so detection on resume can only come from
# the `max_log_ptr > new_max_log_ptr` rollback guard.
$CLICKHOUSE_CLIENT --query "DROP DATABASE $DB SYNC"
$CLICKHOUSE_CLIENT --query "
    CREATE DATABASE $DB ENGINE = Replicated('$ZK_PATH', 's0', 'r0');
" > /dev/null

# Resume the snapshot. It reads `new_max_log_ptr = 1` from the recreated database,
# below the captured `max_log_ptr` (e.g. 11), so the `max_log_ptr > new_max_log_ptr`
# branch throws `Replicated database was dropped ... (log pointer moved from N to 1)`.
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FAILPOINT"

# Wait for the backup to leave `CREATING_BACKUP`. The failpoint fires once and the
# snapshot path fails immediately, so this drain completes well under a second; the
# deadline is just a safety bound.
DEADLINE=$((SECONDS + 30))
while [[ $SECONDS -lt $DEADLINE ]]; do
    STATUS=$($CLICKHOUSE_CLIENT --query "
        SELECT status FROM system.backups WHERE id = '$BACKUP_ID' LIMIT 1
    ")
    [[ "$STATUS" != "CREATING_BACKUP" ]] && break
    sleep 0.1
done

# Regression detection: the pre-fix code paths must not fire (debug-build
# `chassert(max_log_ptr == new_max_log_ptr)` LOGICAL_ERROR, or release-build
# retry-loop exhaustion).
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.backups
    WHERE id = '$BACKUP_ID'
      AND (error LIKE '%LOGICAL_ERROR%'
           OR error LIKE '%Cannot get consistent metadata snapshot%')
"

# Fix engagement: with the pause placed after the entry-time identity check, the only
# detection path left is the rollback guard, so its specific message must fire. The
# `log pointer moved from` text is produced only by the `max_log_ptr > new_max_log_ptr`
# branches, not by the entry-time `czxid` check.
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0 FROM system.backups
    WHERE id = '$BACKUP_ID'
      AND error LIKE '%Replicated database was dropped%'
      AND error LIKE '%log pointer moved from%'
"

# Liveness check: the server is still alive.
$CLICKHOUSE_CLIENT --query "SELECT 1"
