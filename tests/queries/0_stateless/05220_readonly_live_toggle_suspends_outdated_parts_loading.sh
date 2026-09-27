#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: the failpoint pauses the loading of outdated parts of every table.
# no-replicated-database, no-shared-merge-tree: `table_readonly` is a plain MergeTree setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# A fail point is server-global state: disarm every one this test enables on any path out of it,
# so that a paused part loader is released and nothing fires in a concurrently running test.
function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_loading_outdated_part" 2>/dev/null || true
}
trap cleanup EXIT

# A writable table loads its outdated parts asynchronously after start. Loading modifies the disk
# (it detaches broken parts, removes duplicates, and prepares parts for removal), so a table that is
# made read-only with `ALTER TABLE ... MODIFY SETTING table_readonly = 1` while that loading is still
# pending must not go on loading afterwards: the remaining parts stay unloaded on disk until the
# setting is turned off again.
#
# The loader is paused right before it takes its first part, the table is made read-only meanwhile,
# and the loader is then released. It must load nothing, and a later toggle back must load the parts.

$CLICKHOUSE_CLIENT --multiquery -q "
    DROP TABLE IF EXISTS readonly_live_toggle_outdated SYNC;
    CREATE TABLE readonly_live_toggle_outdated (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS old_parts_lifetime = 3600, min_bytes_for_wide_part = 0;
    SYSTEM STOP CLEANUP readonly_live_toggle_outdated;
    INSERT INTO readonly_live_toggle_outdated SELECT number FROM numbers(10);
    INSERT INTO readonly_live_toggle_outdated SELECT number + 10 FROM numbers(10);
    -- The two original parts remain outdated on disk next to the merged part.
    OPTIMIZE TABLE readonly_live_toggle_outdated FINAL;
    DETACH TABLE readonly_live_toggle_outdated;
"

function inactive_parts()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts
        WHERE database = currentDatabase() AND table = 'readonly_live_toggle_outdated' AND NOT active"
}

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_pause_before_loading_outdated_part"
$CLICKHOUSE_CLIENT --multiquery -q "
    ATTACH TABLE readonly_live_toggle_outdated;
    SYSTEM STOP CLEANUP readonly_live_toggle_outdated;
"

# The loader of the writable table is about to take its first outdated part.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_pause_before_loading_outdated_part PAUSE"
echo "outdated parts loaded before the toggle: $(inactive_parts)"

# Making the table read-only does not wait for the pending loader, it disables it.
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_live_toggle_outdated MODIFY SETTING table_readonly = 1"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_loading_outdated_part"

# The released loader must stop instead of loading. Give it ample time to load if it were allowed to.
sleep 2
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT LOADING PARTS readonly_live_toggle_outdated"
echo "outdated parts loaded after the toggle: $(inactive_parts)"
$CLICKHOUSE_CLIENT -q "INSERT INTO readonly_live_toggle_outdated VALUES (100)" 2>&1 \
    | grep -q -F 'TABLE_IS_PERMANENTLY_READ_ONLY' && echo 'table is readonly: 1'

# Turning the setting off resumes the loading. Cleanup is stopped, so both outdated parts exist.
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_live_toggle_outdated MODIFY SETTING table_readonly = 0"
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT LOADING PARTS readonly_live_toggle_outdated"
echo "outdated parts loaded after toggling back: $(inactive_parts)"
echo "rows after toggling back: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_live_toggle_outdated')"

$CLICKHOUSE_CLIENT --multiquery -q "
    SYSTEM START CLEANUP readonly_live_toggle_outdated;
    DROP TABLE readonly_live_toggle_outdated SYNC;
"
