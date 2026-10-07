#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: the failpoint pauses the loading of outdated parts of every table.
# no-replicated-database, no-shared-merge-tree: `table_readonly` is a plain MergeTree setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# A fail point is server-global state: disarm every one this test enables on any path out of it,
# so that paused part loads are released and nothing fires in a concurrently running test.
function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_loading_queued_outdated_part" 2>/dev/null || true
}
trap cleanup EXIT

# A writable table loads its outdated parts asynchronously after start: a scheduling loop hands the
# parts one by one to a thread pool, which loads a bounded number of them at a time. The loop can
# therefore be far ahead of the pool, with several loads queued but not started. Loading modifies the
# disk (it detaches broken parts, removes duplicates, and prepares parts for removal), so a table that
# is made read-only with `ALTER TABLE ... MODIFY SETTING table_readonly = 1` while such loads are queued
# must not run them afterwards, not only stop handing out further parts: every queued load that has
# not started yet is put back, and the parts stay unloaded on disk until the setting is turned off.
#
# Every queued load is paused right before it starts, the table is made read-only meanwhile, and the
# loads are then released. None of them may load, and a later toggle back must load all the parts.

$CLICKHOUSE_CLIENT --multiquery -q "
    DROP TABLE IF EXISTS readonly_queued_outdated SYNC;
    CREATE TABLE readonly_queued_outdated (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS old_parts_lifetime = 3600, min_bytes_for_wide_part = 0;
    SYSTEM STOP CLEANUP readonly_queued_outdated;
    INSERT INTO readonly_queued_outdated SELECT number FROM numbers(10);
    INSERT INTO readonly_queued_outdated SELECT number + 10 FROM numbers(10);
    INSERT INTO readonly_queued_outdated SELECT number + 20 FROM numbers(10);
    INSERT INTO readonly_queued_outdated SELECT number + 30 FROM numbers(10);
    -- The four original parts remain outdated on disk next to the merged part.
    OPTIMIZE TABLE readonly_queued_outdated FINAL;
    DETACH TABLE readonly_queued_outdated;
"

function inactive_parts()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts
        WHERE database = currentDatabase() AND table = 'readonly_queued_outdated' AND NOT active"
}

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_pause_before_loading_queued_outdated_part"
$CLICKHOUSE_CLIENT --multiquery -q "
    ATTACH TABLE readonly_queued_outdated;
    SYSTEM STOP CLEANUP readonly_queued_outdated;
"

# The scheduling loop hands every outdated part to the pool; the queued loads pause before they start.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_pause_before_loading_queued_outdated_part PAUSE"
echo "outdated parts loaded before the toggle: $(inactive_parts)"

# Making the table read-only does not wait for the queued loads, it disables them.
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_queued_outdated MODIFY SETTING table_readonly = 1"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_loading_queued_outdated_part"

# The released loads must put their parts back instead of loading. Give them ample time to load if
# they were allowed to.
sleep 2
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT LOADING PARTS readonly_queued_outdated"
echo "outdated parts loaded after the toggle: $(inactive_parts)"
$CLICKHOUSE_CLIENT -q "INSERT INTO readonly_queued_outdated VALUES (100)" 2>&1 \
    | grep -q -F 'TABLE_IS_PERMANENTLY_READ_ONLY' && echo 'table is readonly: 1'

# Turning the setting off resumes the loading. Cleanup is stopped, so all four outdated parts exist.
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_queued_outdated MODIFY SETTING table_readonly = 0"
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT LOADING PARTS readonly_queued_outdated"
echo "outdated parts loaded after toggling back: $(inactive_parts)"
echo "rows after toggling back: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_queued_outdated')"

$CLICKHOUSE_CLIENT --multiquery -q "
    SYSTEM START CLEANUP readonly_queued_outdated;
    DROP TABLE readonly_queued_outdated SYNC;
"
