#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: the failpoints apply to settings-only ALTERs of all tables.
# no-replicated-database, no-shared-merge-tree: `table_readonly` is a plain MergeTree setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# A fail point is server-global state: disarm every one this test enables on any path out of it,
# so that a paused `ALTER` is released and nothing fires in a concurrently running test.
function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_alter_settings_pause_before_metadata_commit" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_alter_settings_throw_before_metadata_commit" 2>/dev/null || true
}
trap cleanup EXIT

# `ALTER TABLE ... MODIFY SETTING table_readonly = 1` on a writable table makes the new value visible
# in memory before the metadata commit. A background worker that wakes up in that window sees a
# read-only table, finds nothing to do, and goes into its backoff, which grows up to minutes. If the
# commit then fails, the rollback leaves the table writable, but the worker keeps sleeping, with a
# merge, mutation, or move possibly pending. The rollback must wake the workers up again, so that the
# pending work resumes without waiting for the backoff or a manual `SYSTEM START MERGES`.
#
# A mutation is left pending, the ALTER is paused right before the commit, the merge/mutate assignee
# is woken up inside the window (and goes to sleep, as the table is read-only for it), and the commit
# then fails. The mutation must execute right after the failed ALTER.

$CLICKHOUSE_CLIENT --multiquery -q "
    DROP TABLE IF EXISTS readonly_rollback_wakeup SYNC;
    CREATE TABLE readonly_rollback_wakeup (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO readonly_rollback_wakeup SELECT number FROM numbers(10);
    SYSTEM STOP MERGES readonly_rollback_wakeup;
    ALTER TABLE readonly_rollback_wakeup DELETE WHERE k = 0 SETTINGS mutations_sync = 0;
"

$CLICKHOUSE_CLIENT -q "SELECT 'mutation pending: ' || toString(is_done = 0)
    FROM system.mutations WHERE database = currentDatabase() AND table = 'readonly_rollback_wakeup'"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_alter_settings_pause_before_metadata_commit"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_alter_settings_throw_before_metadata_commit"

$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_rollback_wakeup MODIFY SETTING table_readonly = 1" > "${CLICKHOUSE_TMP}/05225_alter.out" 2>&1 &
alter_pid=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_alter_settings_pause_before_metadata_commit PAUSE"

# The table is read-only in memory but not durably. Wake the merge/mutate assignee up: it runs right
# away, must not execute the mutation, and goes into its backoff.
$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES readonly_rollback_wakeup"
# Give the woken worker ample time to run, and to execute the mutation if it were allowed to.
sleep 2
$CLICKHOUSE_CLIENT -q "SELECT 'mutation still pending inside the window: ' || toString(is_done = 0)
    FROM system.mutations WHERE database = currentDatabase() AND table = 'readonly_rollback_wakeup'"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_alter_settings_pause_before_metadata_commit"
wait $alter_pid || true
grep -q -F 'FAULT_INJECTED' "${CLICKHOUSE_TMP}/05225_alter.out" && echo 'toggle failed at the commit: 1'

# The rollback left the table writable and woke the assignee up: the mutation executes now, well
# before the backoff of at least ten seconds that the worker went into inside the window would expire.
done_in_background=0
for _ in $(seq 1 80); do
    if [[ "$($CLICKHOUSE_CLIENT -q "SELECT is_done FROM system.mutations
                WHERE database = currentDatabase() AND table = 'readonly_rollback_wakeup'")" == "1" ]]; then
        done_in_background=1
        break
    fi
    sleep 0.1
done
echo "mutation executed after failed toggle: $done_in_background"
echo "rows after failed toggle: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_rollback_wakeup')"
$CLICKHOUSE_CLIENT -q "INSERT INTO readonly_rollback_wakeup VALUES (100)"
echo "rows after insert into the writable table: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_rollback_wakeup')"

$CLICKHOUSE_CLIENT -q "DROP TABLE readonly_rollback_wakeup SYNC"
