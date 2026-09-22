#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: the failpoint applies to `table_readonly` 1 -> 0 ALTERs of all tables.
# no-replicated-database, no-shared-merge-tree: `table_readonly` is a plain MergeTree setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# A fail point is server-global state: disarm every one this test enables on any path out of it,
# so that a paused `ALTER` is released and nothing fires in a concurrently running test.
function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_alter_readonly_pause_after_metadata_commit" 2>/dev/null || true
    # Release and reap the `ALTER` that a paused fail point may have left parked in the background.
    if [[ -n "${alter_pid:-}" ]]
    then
        wait "$alter_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# A successful `table_readonly` 1 -> 0 toggle commits the setting before it restores the background
# workers: enabling them and rescheduling the outdated-part loaders happens in the tail of
# `StorageMergeTree::alter`, after the metadata commit. In that gap the table is durably writable
# while `waitForOutdatedPartsToBeLoaded` still takes its "nothing is loading" fast path, so a
# partition command that ran there would act on a table whose outdated parts are not loaded yet.
#
# The toggle is paused in exactly that gap. A `MOVE PARTITION TO TABLE` into the table, which checks
# the destination without taking its `alter_lock` and is therefore the one partition command that
# does not simply block on the running `ALTER`, must still be rejected.

$CLICKHOUSE_CLIENT --multiquery -q "
    DROP TABLE IF EXISTS readonly_tail_dest SYNC;
    DROP TABLE IF EXISTS readonly_tail_source SYNC;
    CREATE TABLE readonly_tail_dest (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k % 2;
    CREATE TABLE readonly_tail_source (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k % 2;
    INSERT INTO readonly_tail_dest SELECT number FROM numbers(10);
    INSERT INTO readonly_tail_source VALUES (100), (101);
    ALTER TABLE readonly_tail_dest MODIFY SETTING table_readonly = 1;
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_alter_readonly_pause_after_metadata_commit"

$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_tail_dest MODIFY SETTING table_readonly = 0" &
alter_pid=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_alter_readonly_pause_after_metadata_commit PAUSE"

# `table_readonly` is durably 0 here, but the background workers are not back yet.
# `timeout` so that a regression which blocks instead of rejecting does not hang the test.
if timeout 30 $CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_tail_source MOVE PARTITION 0 TO TABLE readonly_tail_dest" < /dev/null 2>&1 \
    | grep -q -F 'TABLE_IS_PERMANENTLY_READ_ONLY'
then
    echo "rejected inside the post-commit window: 1"
else
    echo "rejected inside the post-commit window: 0"
fi

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_alter_readonly_pause_after_metadata_commit"
wait $alter_pid

echo "rows after the toggle: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_tail_dest')"
echo "rows left in the source: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_tail_source')"

# Once the toggle returned, the table is writable and the same command succeeds.
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_tail_source MOVE PARTITION 0 TO TABLE readonly_tail_dest" < /dev/null
echo "rows after the move: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_tail_dest')"
echo "rows left in the source after the move: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_tail_source')"

$CLICKHOUSE_CLIENT -q "DROP TABLE readonly_tail_dest SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE readonly_tail_source SYNC"
