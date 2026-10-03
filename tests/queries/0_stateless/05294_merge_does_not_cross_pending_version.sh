#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel -- server-wide failpoints pause the next mutation registration and the next lightweight update.
# no-replicated-database -- the local timing of `DELETE`, `UPDATE` and `OPTIMIZE` is assumed.
# no-shared-merge-tree -- the merge selection under test is StorageMergeTree's.

# Merge selection on a plain `MergeTree` must not cross a mutation version that is allocated but not
# registered yet (arm 1). Merging parts that both lie below a pending update is legitimate (arm 2): the patch
# is matched to the merged part by its block number and offset columns, not by part identity, so it still applies.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_reservations.lib
. "$CUR_DIR"/mergetree_reservations.lib

MUTATION_FP="mt_pause_before_register_mutation"
UPDATE_FP="mt_lightweight_update_pause_after_block_allocation"
TABLE="t_merge_pending"
MUTATIONS="FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE'"

function cleanup()
{
    for fp in $MUTATION_FP $UPDATE_FP; do
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $fp"
    done
    wait
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null
}
trap cleanup EXIT

echo "--- a merge does not cross a mutation that is paused before registration"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $TABLE SYNC;
    CREATE TABLE $TABLE (id UInt64) ENGINE = MergeTree ORDER BY id;
    SYSTEM STOP MERGES $TABLE;
    INSERT INTO $TABLE VALUES (0);
"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $MUTATION_FP"
$CLICKHOUSE_CLIENT --lightweight_deletes_sync=0 --lightweight_delete_mode=alter_update --query "DELETE FROM $TABLE WHERE id = 1" &
paused_pid=$!
wait_failpoint $MUTATION_FP
# This row's block number is above the pending mutation: the mutation must not apply to it.
$CLICKHOUSE_CLIENT --query "INSERT INTO $TABLE VALUES (1)"
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES $TABLE"
# `OPTIMIZE` must refuse: the two parts have an allocated but not yet visible version between their data versions.
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $TABLE FINAL SETTINGS optimize_throw_if_noop = 1" 2>&1 | expect_error "CANNOT_ASSIGN_OPTIMIZE" "between them that is not visible yet"
$CLICKHOUSE_CLIENT --query "SELECT 'active parts:', count() FROM system.parts WHERE database = currentDatabase() AND table = '$TABLE' AND active AND NOT startsWith(partition_id, 'patch-')"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $MUTATION_FP"
wait "$paused_pid"
wait_for_query_result "SELECT countIf(is_done) $MUTATIONS" "1"
$CLICKHOUSE_CLIENT --query "SELECT 'rows:', groupArray(id) FROM (SELECT id FROM $TABLE ORDER BY id)"

echo "--- a merge of parts below a pending update keeps the update applicable"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE $TABLE SYNC;
    CREATE TABLE $TABLE (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0;
    INSERT INTO $TABLE SELECT number, 1 FROM numbers(1000);
    INSERT INTO $TABLE SELECT number + 1000, 1 FROM numbers(1000);
"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $UPDATE_FP"
$CLICKHOUSE_CLIENT --enable_lightweight_update 1 --query "UPDATE $TABLE SET v = 2 WHERE 1" &
update_pid=$!
wait_failpoint $UPDATE_FP
# Both source parts have a lower data version than the pending update's block, so the merge is not
# refused: the update is keyed by the block number/offset columns, not by part identity, so it still
# finds and applies to the merged part once it commits.
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $TABLE FINAL"
$CLICKHOUSE_CLIENT --query "SELECT 'updated rows before release:', count() FROM $TABLE WHERE v = 2"
$CLICKHOUSE_CLIENT --query "SELECT 'active parts:', count() FROM system.parts WHERE database = currentDatabase() AND table = '$TABLE' AND active AND NOT startsWith(partition_id, 'patch-')"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $UPDATE_FP"
wait "$update_pid"
$CLICKHOUSE_CLIENT --query "SELECT 'updated rows after release:', count() FROM $TABLE WHERE v = 2"
$CLICKHOUSE_CLIENT --query "SELECT 'active parts:', count() FROM system.parts WHERE database = currentDatabase() AND table = '$TABLE' AND active AND NOT startsWith(partition_id, 'patch-')"
