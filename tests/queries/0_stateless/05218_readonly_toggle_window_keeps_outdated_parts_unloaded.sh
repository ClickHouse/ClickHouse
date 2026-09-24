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

# A table attached read-only retains its outdated parts unloaded on disk. The `table_readonly`
# 1 -> 0 ALTER starts the loading task before the metadata commit, inside the rollback unit, because
# starting may throw. Loading modifies the disk (it detaches broken parts, removes duplicates, and
# prepares parts for removal), so the started task must load nothing until the commit succeeded:
# otherwise a failed commit leaves a table that is durably read-only with a modified disk.
#
# The ALTER is paused right before the commit, giving the loader ample time to run, and the commit
# then fails. The outdated parts must still be unloaded, also after the failed toggle, and a retry
# must load them.

$CLICKHOUSE_CLIENT --multiquery -q "
    DROP TABLE IF EXISTS readonly_outdated_window SYNC;
    CREATE TABLE readonly_outdated_window (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS old_parts_lifetime = 3600, min_bytes_for_wide_part = 0;
    SYSTEM STOP CLEANUP readonly_outdated_window;
    INSERT INTO readonly_outdated_window SELECT number FROM numbers(10);
    INSERT INTO readonly_outdated_window SELECT number + 10 FROM numbers(10);
    OPTIMIZE TABLE readonly_outdated_window FINAL;
    -- The merged part is removed by TRUNCATE; its two original parts remain outdated on disk.
    TRUNCATE TABLE readonly_outdated_window;
    ALTER TABLE readonly_outdated_window MODIFY SETTING table_readonly = 1;
    DETACH TABLE readonly_outdated_window;
    ATTACH TABLE readonly_outdated_window;
    SYSTEM STOP CLEANUP readonly_outdated_window;
"

function inactive_parts()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts
        WHERE database = currentDatabase() AND table = 'readonly_outdated_window' AND NOT active"
}

echo "outdated parts loaded on a read-only table: $(inactive_parts)"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_alter_settings_pause_before_metadata_commit"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_alter_settings_throw_before_metadata_commit"

$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_outdated_window MODIFY SETTING table_readonly = 0" > "${CLICKHOUSE_TMP}/05218_alter.out" 2>&1 &
alter_pid=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_alter_settings_pause_before_metadata_commit PAUSE"

# The table is writable in memory but not durably, and the loading task is started. Give it ample
# time to load the outdated parts if it were allowed to. Nothing is loading, so waiting for the
# loading must return at once instead of waiting for the commit.
sleep 2
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT LOADING PARTS readonly_outdated_window"
echo "outdated parts loaded inside the window: $(inactive_parts)"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_alter_settings_pause_before_metadata_commit"
wait $alter_pid || true
grep -q -F 'FAULT_INJECTED' "${CLICKHOUSE_TMP}/05218_alter.out" && echo 'toggle failed at the commit: 1'

# The rollback left the table read-only. The started loading task re-arms itself while the table
# looks writable, and must stay idle now that it is read-only again.
$CLICKHOUSE_CLIENT -q "INSERT INTO readonly_outdated_window VALUES (100)" 2>&1 \
    | grep -q -F 'TABLE_IS_PERMANENTLY_READ_ONLY' && echo 'still readonly after failed toggle: 1'
sleep 3
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT LOADING PARTS readonly_outdated_window"
echo "outdated parts loaded after failed toggle: $(inactive_parts)"

# A successful toggle loads them. Cleanup is stopped, so the empty cover and both outdated parts exist.
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_outdated_window MODIFY SETTING table_readonly = 0"
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT LOADING PARTS readonly_outdated_window"
$CLICKHOUSE_CLIENT -q "SELECT 'parts after successful toggle: ' || toString(countIf(active AND rows = 0)) || ' empty active, '
        || toString(countIf(NOT active AND rows > 0)) || ' outdated'
    FROM system.parts WHERE database = currentDatabase() AND table = 'readonly_outdated_window'"
echo "rows after successful toggle: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_outdated_window')"

$CLICKHOUSE_CLIENT --multiquery -q "
    SYSTEM START CLEANUP readonly_outdated_window;
    DROP TABLE readonly_outdated_window SYNC;
"
