#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: the failpoints apply to settings-only ALTERs and to the outdated part loaders of all tables.
# no-replicated-database, no-shared-merge-tree: `table_readonly` is a plain MergeTree setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# A fail point is server-global state: disarm every one this test enables on any path out of it,
# so that a paused `ALTER` or part loader is released and nothing fires in a concurrently running test.
function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_loading_outdated_part" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_alter_settings_pause_before_metadata_commit" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_alter_settings_throw_before_metadata_commit" 2>/dev/null || true
    # Release and reap the `ALTER` that a paused fail point may have left parked in the background.
    if [[ -n "${alter_pid:-}" ]]
    then
        wait "$alter_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# `ALTER TABLE ... MODIFY SETTING table_readonly = 1` on a writable table must disable its background
# workers *before* the metadata commit, not after it. The cleanup thread and the asynchronous loader
# of outdated parts, which a writable table starts after attach, are gated by that switch alone, so a
# loader released between the durable commit and a switch flipped afterwards would still modify the
# disk of a table that is already durably read-only.
#
# The loader of a freshly attached table is paused right before it takes its first part, the toggle
# is paused right before its commit, and the loader is released inside that window. It must load
# nothing, neither in the window nor after the commit, and a toggle back must load the parts.
# When the commit fails instead, the rollback leaves the table writable and the loader must resume
# on its own, without a further toggle.

function inactive_parts()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts
        WHERE database = currentDatabase() AND table = 'readonly_disable_before_commit' AND NOT active"
}

function attach_with_paused_loader()
{
    $CLICKHOUSE_CLIENT --multiquery -q "
        DROP TABLE IF EXISTS readonly_disable_before_commit SYNC;
        CREATE TABLE readonly_disable_before_commit (x UInt64) ENGINE = MergeTree ORDER BY x
        SETTINGS old_parts_lifetime = 3600, min_bytes_for_wide_part = 0;
        SYSTEM STOP CLEANUP readonly_disable_before_commit;
        INSERT INTO readonly_disable_before_commit SELECT number FROM numbers(10);
        INSERT INTO readonly_disable_before_commit SELECT number + 10 FROM numbers(10);
        -- The two original parts remain outdated on disk next to the merged part.
        OPTIMIZE TABLE readonly_disable_before_commit FINAL;
        DETACH TABLE readonly_disable_before_commit;
    "
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_pause_before_loading_outdated_part"
    $CLICKHOUSE_CLIENT --multiquery -q "
        ATTACH TABLE readonly_disable_before_commit;
        SYSTEM STOP CLEANUP readonly_disable_before_commit;
    "
    # The loader of the writable table is about to take its first outdated part.
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_pause_before_loading_outdated_part PAUSE"
    echo "outdated parts loaded before the toggle: $(inactive_parts)"
}

echo "--- successful toggle"
attach_with_paused_loader

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_alter_settings_pause_before_metadata_commit"
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_disable_before_commit MODIFY SETTING table_readonly = 1" &
alter_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_alter_settings_pause_before_metadata_commit PAUSE"

# The table is read-only in memory but not durably, and its workers are already disabled. The
# released loader must return without loading. Give it ample time to load if it were allowed to.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_loading_outdated_part"
sleep 2
echo "outdated parts loaded inside the window: $(inactive_parts)"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_alter_settings_pause_before_metadata_commit"
wait $alter_pid
alter_pid=""

sleep 2
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT LOADING PARTS readonly_disable_before_commit"
echo "outdated parts loaded after the toggle: $(inactive_parts)"
$CLICKHOUSE_CLIENT -q "INSERT INTO readonly_disable_before_commit VALUES (100)" 2>&1 \
    | grep -q -F 'TABLE_IS_PERMANENTLY_READ_ONLY' && echo 'table is readonly: 1'

# Turning the setting off resumes the loading. Cleanup is stopped, so both outdated parts exist.
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_disable_before_commit MODIFY SETTING table_readonly = 0"
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT LOADING PARTS readonly_disable_before_commit"
echo "outdated parts loaded after toggling back: $(inactive_parts)"
echo "rows after toggling back: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_disable_before_commit')"

echo "--- rolled back toggle"
attach_with_paused_loader

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_alter_settings_pause_before_metadata_commit"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_alter_settings_throw_before_metadata_commit"
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_disable_before_commit MODIFY SETTING table_readonly = 1" > "${CLICKHOUSE_TMP}/05238_alter.out" 2>&1 &
alter_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_alter_settings_pause_before_metadata_commit PAUSE"

# The released loader finds the workers disabled and, as the table reads as read-only, does not
# re-arm itself.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_loading_outdated_part"
sleep 2
echo "outdated parts loaded inside the window: $(inactive_parts)"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_alter_settings_pause_before_metadata_commit"
wait $alter_pid || true
alter_pid=""
grep -q -F 'FAULT_INJECTED' "${CLICKHOUSE_TMP}/05238_alter.out" && echo 'toggle failed at the commit: 1'

# The rollback left the table writable and scheduled the loader again: the parts load now.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT LOADING PARTS readonly_disable_before_commit"
echo "outdated parts loaded after the failed toggle: $(inactive_parts)"
$CLICKHOUSE_CLIENT -q "INSERT INTO readonly_disable_before_commit VALUES (100)"
echo "rows after insert into the writable table: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_disable_before_commit')"

$CLICKHOUSE_CLIENT --multiquery -q "
    SYSTEM START CLEANUP readonly_disable_before_commit;
    DROP TABLE readonly_disable_before_commit SYNC;
"
