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

# `ALTER TABLE ... MODIFY SETTING table_readonly = 0` publishes the new setting in memory before the
# metadata commit. The table is durably read-only until the commit succeeds, and the `ALTER` only
# holds `alter_lock`, which does not serialize with the `lockForShare` that the write paths take. A
# foreground query that passed `assertNotReadonly` in that window would modify a table whose failed
# commit restores `table_readonly = 1`, leaving new data on a durably read-only table.
#
# The 1 -> 0 toggle is paused right before the commit. Two queries that do not take the paused
# table's `alter_lock` must still be rejected inside the window: an `INSERT`, and a
# `MOVE PARTITION TO TABLE` from another table, which checks the destination without locking it.

$CLICKHOUSE_CLIENT --multiquery -q "
    DROP TABLE IF EXISTS readonly_window_writes SYNC;
    DROP TABLE IF EXISTS readonly_window_source SYNC;
    CREATE TABLE readonly_window_writes (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k % 2;
    CREATE TABLE readonly_window_source (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k % 2;
    INSERT INTO readonly_window_writes SELECT number FROM numbers(10);
    INSERT INTO readonly_window_source VALUES (100), (102);
    ALTER TABLE readonly_window_writes MODIFY SETTING table_readonly = 1;
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_alter_settings_pause_before_metadata_commit"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_alter_settings_throw_before_metadata_commit"

$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_window_writes MODIFY SETTING table_readonly = 0" > "${CLICKHOUSE_TMP}/05233_alter.out" 2>&1 &
alter_pid=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_alter_settings_pause_before_metadata_commit PAUSE"

# `table_readonly` reads 0 in memory here, but the table is not durably writable yet.
queries=(
    "INSERT INTO readonly_window_writes VALUES (100)"
    "ALTER TABLE readonly_window_source MOVE PARTITION 0 TO TABLE readonly_window_writes"
)
for query in "${queries[@]}"
do
    # `timeout` so that a regression which blocks instead of rejecting does not hang the test, and
    # `< /dev/null` so that an `INSERT ... VALUES` that is not rejected does not read the rest of
    # the script's input as its data.
    if timeout 30 $CLICKHOUSE_CLIENT -q "$query" < /dev/null 2>&1 | grep -q -F 'TABLE_IS_PERMANENTLY_READ_ONLY'; then
        echo "rejected inside the window: 1"
    else
        echo "rejected inside the window: 0 -- $query"
    fi
done

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_alter_settings_pause_before_metadata_commit"
wait $alter_pid || true
grep -q -F 'FAULT_INJECTED' "${CLICKHOUSE_TMP}/05233_alter.out" && echo 'toggle failed at the commit: 1'

echo "rows after the failed toggle: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_window_writes')"
echo "rows left in the source: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_window_source')"
$CLICKHOUSE_CLIENT -q "INSERT INTO readonly_window_writes VALUES (100)" < /dev/null 2>&1 | grep -q -F 'TABLE_IS_PERMANENTLY_READ_ONLY' \
    && echo 'still read-only after the failed toggle: 1'

# The retried toggle succeeds and the table accepts writes again.
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_window_writes MODIFY SETTING table_readonly = 0"
$CLICKHOUSE_CLIENT -q "INSERT INTO readonly_window_writes VALUES (100)" < /dev/null
echo "rows after the successful toggle: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_window_writes')"

$CLICKHOUSE_CLIENT -q "DROP TABLE readonly_window_writes SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE readonly_window_source SYNC"
