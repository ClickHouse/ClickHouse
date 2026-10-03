#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel -- server-wide failpoints pause the next lightweight update and the next mutation on any table.
# no-replicated-database -- DELETE and UPDATE run through replicated DDL there, not with the local timing assumed here.
# no-shared-merge-tree -- the ordering under test is the in-memory version bookkeeping of StorageMergeTree.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_reservations.lib
. "$CUR_DIR"/mergetree_reservations.lib

UPDATE_FP="mt_lightweight_update_pause_after_block_allocation"
TABLE="t_pending_versions"
MUTATIONS="FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE'"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $UPDATE_FP"
    wait
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null
}
trap cleanup EXIT

function fresh_table()
{
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS $TABLE SYNC;
        CREATE TABLE $TABLE (id UInt64, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0;
        INSERT INTO $TABLE SELECT number, 1, 0 FROM numbers(1000);
    "
}

echo "--- the guard of the mutation selector does not depend on the table settings"
fresh_table
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $UPDATE_FP"
$CLICKHOUSE_CLIENT --enable_lightweight_update 1 --query "UPDATE $TABLE SET v = 2 WHERE v = 1" &
update_pid=$!
wait_failpoint $UPDATE_FP
# A settings ALTER that makes supportsLightweightUpdate() false must not hide the pending update.
$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE MODIFY SETTING enable_block_offset_column = 0"
$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE UPDATE w = 5 WHERE 1 SETTINGS mutations_sync = 0"
wait_for_query_result "SELECT count() $MUTATIONS AND NOT is_done AND arrayExists(x -> x LIKE 'Lightweight update%', mapValues(parts_postpone_reasons))" "1"
$CLICKHOUSE_CLIENT --query "SELECT 'postponed:', arrayDistinct(mapValues(parts_postpone_reasons)) $MUTATIONS"
$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE MODIFY SETTING enable_block_offset_column = 1"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $UPDATE_FP"
wait "$update_pid"
wait_for_query_result "SELECT count() $MUTATIONS AND NOT is_done" "0"
$CLICKHOUSE_CLIENT --query "SELECT 'update survived', count() FROM $TABLE WHERE v = 2; SELECT 'mutation applied', count() FROM $TABLE WHERE w = 5;"

echo "--- a cancelled pending update releases the postponed mutation"
fresh_table
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $UPDATE_FP"
$CLICKHOUSE_CLIENT --enable_lightweight_update 1 --query_id "05292_cancelled_update_${CLICKHOUSE_DATABASE}" --query "UPDATE $TABLE SET v = 3 WHERE v = 1" 2>/dev/null &
update_pid=$!
wait_failpoint $UPDATE_FP
$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE UPDATE w = 7 WHERE 1 SETTINGS mutations_sync = 0"
wait_for_query_result "SELECT count() $MUTATIONS AND NOT is_done AND arrayExists(x -> x LIKE 'Lightweight update%', mapValues(parts_postpone_reasons))" "1"
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '05292_cancelled_update_${CLICKHOUSE_DATABASE}' ASYNC FORMAT Null"
# A thread parked in a failpoint notices the kill only when the failpoint is disabled; ASYNC
# above is required, since SYNC would wait for the very query this cannot unblock until then.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $UPDATE_FP"
wait "$update_pid"
wait_for_query_result "SELECT count() $MUTATIONS AND NOT is_done" "0"
$CLICKHOUSE_CLIENT --query "SELECT 'no patch committed', count() FROM $TABLE WHERE v = 3; SELECT 'mutation applied', count() FROM $TABLE WHERE w = 7;"

echo "--- a lower registered mutation applies while a higher one waits behind a pending update"
fresh_table
# STOP MERGES also stops mutations on plain MergeTree, so K and M register but do not run until
# U is parked and released; this is what exercises the truncated end bound, not task ordering.
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES $TABLE"
$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE UPDATE w = 1 WHERE 1 SETTINGS mutations_sync = 0"
wait_for_query_result "SELECT count() $MUTATIONS" "1"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $UPDATE_FP"
$CLICKHOUSE_CLIENT --enable_lightweight_update 1 --query "UPDATE $TABLE SET v = 4 WHERE v = 1" &
update_pid=$!
wait_failpoint $UPDATE_FP
$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE UPDATE w = w + 10 WHERE 1 SETTINGS mutations_sync = 0"
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES $TABLE"
# K finishes; M is postponed behind U.
wait_for_query_result "SELECT countIf(is_done) $MUTATIONS" "1"
wait_for_query_result "SELECT count() $MUTATIONS AND NOT is_done AND arrayExists(x -> x LIKE 'Lightweight update%', mapValues(parts_postpone_reasons))" "1"
$CLICKHOUSE_CLIENT --query "SELECT 'after K, before U:', w, count() FROM $TABLE GROUP BY w ORDER BY w"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $UPDATE_FP"
wait "$update_pid"
wait_for_query_result "SELECT count() $MUTATIONS AND NOT is_done" "0"
$CLICKHOUSE_CLIENT --query "SELECT 'after U and M:', v, w, count() FROM $TABLE GROUP BY v, w ORDER BY v, w"
