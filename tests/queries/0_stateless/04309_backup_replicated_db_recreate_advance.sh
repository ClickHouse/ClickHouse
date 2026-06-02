#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-parallel
# no-parallel: the `database_replicated_pause_after_reading_log_pointer` failpoint
#   is `PAUSEABLE_ONCE` and fires globally; a concurrent backup from another test
#   could steal the pause from this test's submitted backup, causing
#   `SYSTEM WAIT FAILPOINT ... PAUSE` to hang or to be unblocked by the wrong query.

# Companion to `04057_backup_replicated_db_recreate.sh`. That test recreates the
# database with a *smaller* `max_log_ptr` (the new instance starts at 1), so the
# drop/recreate race is caught by the `max_log_ptr > new_max_log_ptr` rollback
# guard. That test therefore still passes even if the `expected_max_log_ptr_czxid`
# identity check is removed or wired incorrectly.
#
# This test exercises the harder case the rollback guard cannot catch: the
# recreated database advances its `max_log_ptr` *past* the captured
# `snapshot_version` before the backup resumes. With `new_max_log_ptr >=
# snapshot_version`, the rollback guard never fires; the only thing that detects
# the substitution is the entry-time `czxid` identity check in
# `getConsistentMetadataSnapshotImpl` (the `expected_max_log_ptr_czxid` forwarded
# from `getTablesForBackup`). If that check is removed, this backup would silently
# snapshot the unrelated recreated database instead of failing, and signal #2
# below would become 0.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="db_$CLICKHOUSE_DATABASE"
ZK_PATH="/clickhouse/databases/$DB"
BACKUP_ID="${CLICKHOUSE_DATABASE}_recreate_advance_test"
FAILPOINT="database_replicated_pause_after_reading_log_pointer"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" > /dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT --query "SYSTEM UNFREEZE WITH ID = '$BACKUP_ID'" > /dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS $DB SYNC" > /dev/null 2>&1 ||:
}
trap cleanup EXIT
cleanup

# Create the original database with a few tables so `snapshot_version` is small
# (e.g. 6). We intentionally keep this low so the recreated database below can
# easily advance past it.
$CLICKHOUSE_CLIENT --query "
    CREATE DATABASE $DB ENGINE = Replicated('$ZK_PATH', 's0', 'r0');
" > /dev/null
for i in $(seq 1 5); do
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE $DB.t_$i (x UInt64) ENGINE = MergeTree ORDER BY x;
    " > /dev/null
done

# Arm the failpoint and submit the backup. `BACKUP ... ASYNC` returns immediately;
# the snapshot work runs on a server background thread, which is the thread that
# hits the failpoint inside `getTablesForBackup` after reading `snapshot_version`.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FAILPOINT"
$CLICKHOUSE_CLIENT --query "
    BACKUP DATABASE $DB TO Disk('backups', '$BACKUP_ID')
    SETTINGS id = '$BACKUP_ID' ASYNC
" > /dev/null

# Block until the backup thread is paused at the failpoint.
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"

# Drop and recreate at the same Keeper path, then create *more* tables than the
# original so the new database's `max_log_ptr` ends up strictly greater than the
# captured `snapshot_version`. This is the key difference from `04057`: the
# `max_log_ptr > new_max_log_ptr` rollback guard cannot fire, so detection relies
# entirely on the `czxid` identity check.
$CLICKHOUSE_CLIENT --query "DROP DATABASE $DB SYNC"
$CLICKHOUSE_CLIENT --query "
    CREATE DATABASE $DB ENGINE = Replicated('$ZK_PATH', 's0', 'r0');
" > /dev/null
for i in $(seq 1 20); do
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE $DB.u_$i (x UInt64) ENGINE = MergeTree ORDER BY x;
    " > /dev/null
done

# Resume the backup. It enters `getConsistentMetadataSnapshotImpl` with the old
# `snapshot_version` and the old `czxid`. The recreated `/max_log_ptr` now has a
# different `czxid` and a larger value, so the entry-time identity check throws
# `Replicated database was dropped` before the rollback guard is ever reached.
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FAILPOINT"

# Wait for the backup to leave `CREATING_BACKUP`.
DEADLINE=$((SECONDS + 30))
while [[ $SECONDS -lt $DEADLINE ]]; do
    STATUS=$($CLICKHOUSE_CLIENT --query "
        SELECT status FROM system.backups WHERE id = '$BACKUP_ID' LIMIT 1
    ")
    [[ "$STATUS" != "CREATING_BACKUP" ]] && break
    sleep 0.1
done

# Bugfix-validation signal #1 (regression detection): the pre-fix symptoms must
# not appear.
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.backups
    WHERE id = '$BACKUP_ID'
      AND (error LIKE '%LOGICAL_ERROR%'
           OR error LIKE '%Cannot get consistent metadata snapshot%')
"

# Bugfix-validation signal #2 (czxid identity contract): the recreated database
# advanced past the captured pointer, so the only guard that can reject the
# substitution is the `expected_max_log_ptr_czxid` check. It must fire exactly
# once. If the identity check is removed, the backup would silently succeed on the
# wrong database and this would be 0.
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0 FROM system.backups
    WHERE id = '$BACKUP_ID' AND error LIKE '%Replicated database was dropped%'
"

# Liveness check: the server is still alive.
$CLICKHOUSE_CLIENT --query "SELECT 1"
