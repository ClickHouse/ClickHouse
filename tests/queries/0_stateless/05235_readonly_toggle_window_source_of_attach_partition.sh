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
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_alter_readonly_pause_after_metadata_commit" 2>/dev/null || true
    # Release and reap the `ALTER` that a paused fail point may have left parked in the background.
    if [[ -n "${alter_pid:-}" ]]
    then
        wait "$alter_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# A table attached read-only keeps its outdated parts unloaded on disk. `ATTACH PARTITION ... FROM`
# waits for the outdated parts of the *source* table before cloning, and that wait returns at once
# while the source is read-only, including inside both windows of its `table_readonly` 1 -> 0
# `ALTER`: before the metadata commit, and after it, until the post-commit tail has enabled the
# background workers and rescheduled the part loaders.
#
# Skipping the wait for the source is safe. Cloning reads only the active parts, and the set of
# active parts is complete once the table is attached: the deferred outdated parts are exactly the
# parts covered by an active part. The destination must therefore get all the rows of the source
# in both windows, the source must stay intact, and the toggle must still load its outdated parts.

function parts()
{
    $CLICKHOUSE_CLIENT -q "SELECT toString(countIf(active)) || ' active, ' || toString(countIf(NOT active)) || ' outdated'
        FROM system.parts WHERE database = currentDatabase() AND table = '$1'"
}

function attach_inside_window()
{
    local failpoint=$1
    local dst=$2

    # The merged part is the only active part; its two original parts stay outdated on disk and
    # are not loaded by a read-only attach.
    $CLICKHOUSE_CLIENT --multiquery -q "
        DROP TABLE IF EXISTS readonly_window_src SYNC;
        DROP TABLE IF EXISTS $dst SYNC;
        CREATE TABLE readonly_window_src (x UInt64) ENGINE = MergeTree ORDER BY x
        SETTINGS old_parts_lifetime = 3600, min_bytes_for_wide_part = 0;
        SYSTEM STOP CLEANUP readonly_window_src;
        INSERT INTO readonly_window_src SELECT number FROM numbers(10);
        INSERT INTO readonly_window_src SELECT number + 10 FROM numbers(10);
        OPTIMIZE TABLE readonly_window_src FINAL;
        ALTER TABLE readonly_window_src MODIFY SETTING table_readonly = 1;
        DETACH TABLE readonly_window_src;
        ATTACH TABLE readonly_window_src;
        SYSTEM STOP CLEANUP readonly_window_src;
        CREATE TABLE $dst (x UInt64) ENGINE = MergeTree ORDER BY x
        SETTINGS old_parts_lifetime = 3600, min_bytes_for_wide_part = 0;
    "
    echo "$failpoint"
    echo "source parts before the toggle: $(parts readonly_window_src)"

    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $failpoint"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_window_src MODIFY SETTING table_readonly = 0" &
    alter_pid=$!
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $failpoint PAUSE"

    # The source's `ALTER` holds only the source's `alter_lock`; the destination's command takes the
    # source's `lockForShare`, which does not serialize with it, so it runs inside the window.
    # `timeout` so that a regression which blocks the command until the toggle finishes does not
    # hang the test.
    timeout 30 $CLICKHOUSE_CLIENT -q "ALTER TABLE $dst ATTACH PARTITION ID 'all' FROM readonly_window_src" < /dev/null
    echo "destination rows inside the window: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM $dst")"
    echo "destination parts inside the window: $(parts "$dst")"

    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $failpoint"
    wait $alter_pid

    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT LOADING PARTS readonly_window_src"
    echo "source rows after the toggle: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_window_src')"
    echo "source parts after the toggle: $(parts readonly_window_src)"
    echo "destination rows after the toggle: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM $dst")"

    $CLICKHOUSE_CLIENT --multiquery -q "
        SYSTEM START CLEANUP readonly_window_src;
        DROP TABLE readonly_window_src SYNC;
        DROP TABLE $dst SYNC;
    "
}

attach_inside_window mt_alter_settings_pause_before_metadata_commit readonly_window_dst_pre
attach_inside_window mt_alter_readonly_pause_after_metadata_commit readonly_window_dst_post
