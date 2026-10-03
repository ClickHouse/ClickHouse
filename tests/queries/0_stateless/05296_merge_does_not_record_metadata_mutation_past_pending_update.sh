#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel -- a server-wide failpoint pauses the next lightweight update on any table.
# no-replicated-database -- the local timing of `UPDATE`, `ALTER` and `OPTIMIZE` is assumed.
# no-shared-merge-tree -- the merged part version under test is chosen by StorageMergeTree.

# A merge that materializes a registered metadata mutation (`DROP COLUMN`, `RENAME COLUMN`) records
# that mutation's version in the merged part. A merged part carries one data version, so it must not
# claim a version above a lightweight update that has its block number but has not committed its
# patch yet: the patch would then be skipped for the merged part and the update would be lost.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_reservations.lib
. "$CUR_DIR"/mergetree_reservations.lib

UPDATE_FP="mt_lightweight_update_pause_after_block_allocation"
TABLE="t_merge_past_pending_update"
MUTATIONS="FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE'"
ACTIVE_PARTS="FROM system.parts WHERE database = currentDatabase() AND table = '$TABLE' AND active AND NOT startsWith(partition_id, 'patch-')"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $UPDATE_FP"
    wait
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC"
}
trap cleanup EXIT

function fresh_table()
{
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS $TABLE SYNC;
        CREATE TABLE $TABLE (id UInt64, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0;
        INSERT INTO $TABLE SELECT number, 1, 7 FROM numbers(1000);
        INSERT INTO $TABLE SELECT number + 1000, 1, 7 FROM numbers(1000);
    "
}

function park_update()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $UPDATE_FP"
    $CLICKHOUSE_CLIENT --enable_lightweight_update 1 --query "UPDATE $TABLE SET v = 2 WHERE 1" &
    update_pid=$!
    wait_failpoint $UPDATE_FP
}

function release_update()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $UPDATE_FP"
    wait "$update_pid"
    wait_for_query_result "SELECT count() $MUTATIONS AND NOT is_done" "0"
}

echo "--- a merge does not record a DROP COLUMN registered above a pending update"
fresh_table
park_update
# The DROP COLUMN registers a mutation above the update's block. It is postponed for both parts,
# because the pending update is a boundary for the mutation selector.
$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE DROP COLUMN w SETTINGS alter_sync = 0"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $TABLE FINAL"
$CLICKHOUSE_CLIENT --query "SELECT 'active parts:', count() $ACTIVE_PARTS"
release_update
$CLICKHOUSE_CLIENT --query "SELECT 'rows by v:', v, count() FROM $TABLE GROUP BY v ORDER BY v"
$CLICKHOUSE_CLIENT --query "SELECT 'active parts:', count() $ACTIVE_PARTS"

echo "--- a merge that would materialize a RENAME COLUMN registered above a pending update is refused"
fresh_table
park_update
$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE RENAME COLUMN w TO w2 SETTINGS alter_sync = 0"
# The merge would write `w2` under the new metadata while its part could only record a version below
# the pending update, so the rename would run a second time over the renamed column later.
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $TABLE FINAL SETTINGS optimize_throw_if_noop = 1" 2>&1 | expect_error "CANNOT_ASSIGN_OPTIMIZE" "would materialize a pending RENAME COLUMN"
release_update
$CLICKHOUSE_CLIENT --query "SELECT 'rows by v, w2:', v, w2, count() FROM $TABLE GROUP BY v, w2 ORDER BY v, w2"
