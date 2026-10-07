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

# A settings ALTER makes the new `table_readonly` value visible in memory before the metadata
# commit. In that window the table looks writable to its background workers, which are still running
# for a table that was created writable and later made read-only. A worker that wakes up there must
# not act: otherwise it can execute a mutation (or a merge or a move) that survives a failed commit,
# on a table that the rollback has left read-only.
#
# The ALTER is paused right before the commit, the merge/mutate assignee is woken up explicitly,
# and the commit then fails. The pending mutation must still be pending.

$CLICKHOUSE_CLIENT --multiquery -q "
    DROP TABLE IF EXISTS readonly_window SYNC;
    CREATE TABLE readonly_window (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO readonly_window SELECT number FROM numbers(10);
    SYSTEM STOP MERGES readonly_window;
    ALTER TABLE readonly_window DELETE WHERE k = 0 SETTINGS mutations_sync = 0;
    ALTER TABLE readonly_window MODIFY SETTING table_readonly = 1;
    SYSTEM START MERGES readonly_window;
"

$CLICKHOUSE_CLIENT -q "SELECT 'mutation pending on the read-only table: ' || toString(is_done = 0)
    FROM system.mutations WHERE database = currentDatabase() AND table = 'readonly_window'"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_alter_settings_pause_before_metadata_commit"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_alter_settings_throw_before_metadata_commit"

$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_window MODIFY SETTING table_readonly = 0" > "${CLICKHOUSE_TMP}/05217_alter.out" 2>&1 &
alter_pid=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_alter_settings_pause_before_metadata_commit PAUSE"

# The table is writable in memory but not durably. Wake the merge/mutate assignee up: it runs
# right away and must find nothing to do although `table_readonly` currently reads as 0.
$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES readonly_window"
# Give a woken worker ample time to execute the mutation if it were allowed to.
sleep 2
$CLICKHOUSE_CLIENT -q "SELECT 'mutation still pending inside the window: ' || toString(is_done = 0)
    FROM system.mutations WHERE database = currentDatabase() AND table = 'readonly_window'"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_alter_settings_pause_before_metadata_commit"
wait $alter_pid || true
grep -q -F 'FAULT_INJECTED' "${CLICKHOUSE_TMP}/05217_alter.out" && echo 'toggle failed at the commit: 1'

# The rollback left the table read-only, and the mutation was not executed on it.
$CLICKHOUSE_CLIENT -q "INSERT INTO readonly_window VALUES (100)" 2>&1 \
    | grep -q -F 'TABLE_IS_PERMANENTLY_READ_ONLY' && echo 'still readonly after failed toggle: 1'
$CLICKHOUSE_CLIENT -q "SELECT 'mutation still pending after failed toggle: ' || toString(is_done = 0)
    FROM system.mutations WHERE database = currentDatabase() AND table = 'readonly_window'"
echo "rows after failed toggle: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_window')"

# A successful toggle enables the workers again and the mutation executes.
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_window MODIFY SETTING table_readonly = 0"
done_in_background=0
for _ in $(seq 1 600); do
    if [[ "$($CLICKHOUSE_CLIENT -q "SELECT is_done FROM system.mutations
                WHERE database = currentDatabase() AND table = 'readonly_window'")" == "1" ]]; then
        done_in_background=1
        break
    fi
    sleep 0.1
done
echo "mutation executed after successful toggle: $done_in_background"
echo "rows after successful toggle: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_window')"

$CLICKHOUSE_CLIENT -q "DROP TABLE readonly_window SYNC"
