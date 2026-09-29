#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel -- server-wide failpoints affect the next mutation on any table.
# no-replicated-database -- the local timing of DELETE, UPDATE and OPTIMIZE is assumed.
# no-shared-merge-tree -- the bookkeeping under test is StorageMergeTree's.

# A mutation that throws after allocating its block must release it (a leaked block would refuse every later
# merge across it); `KILL MUTATION` of a registered mutation must not disturb a mutation paused before registration.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_reservations.lib
. "$CUR_DIR"/mergetree_reservations.lib

MUTATION_FP="mt_pause_before_register_mutation"
THROW_FP="mt_throw_after_mutation_commit"
TABLE="t_pending_cleanup"
MUTATIONS="FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE'"

function cleanup()
{
    for fp in $MUTATION_FP $THROW_FP; do
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $fp"
    done
    wait
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null
}
trap cleanup EXIT

echo "--- a mutation that fails after its file is written stops being a boundary"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $TABLE SYNC;
    CREATE TABLE $TABLE (id UInt64) ENGINE = MergeTree ORDER BY id;
    SYSTEM STOP MERGES $TABLE;
    INSERT INTO $TABLE VALUES (0);
"
# The throw fires after the file commit, i.e. after the version is allocated and its file written.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $THROW_FP"
$CLICKHOUSE_CLIENT --lightweight_deletes_sync=0 --lightweight_delete_mode=alter_update --query "DELETE FROM $TABLE WHERE id = 1" 2>&1 | grep -m1 -o "FAULT_INJECTED"
$CLICKHOUSE_CLIENT --query "INSERT INTO $TABLE VALUES (1)"
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES $TABLE"
# No allocated but uncommitted version remains: the two parts merge.
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $TABLE FINAL SETTINGS optimize_throw_if_noop = 1"
$CLICKHOUSE_CLIENT --query "SELECT 'active parts:', count() FROM system.parts WHERE database = currentDatabase() AND table = '$TABLE' AND active AND NOT startsWith(partition_id, 'patch-'); SELECT 'mutations:', count() $MUTATIONS"

echo "--- KILL MUTATION of a registered mutation leaves a later pending one intact"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE $TABLE SYNC;
    CREATE TABLE $TABLE (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO $TABLE SELECT number, 0 FROM numbers(100);
    SYSTEM STOP MERGES $TABLE;
"
$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE UPDATE v = 1 WHERE 1 SETTINGS mutations_sync = 0"
wait_for_query_result "SELECT count() $MUTATIONS" "1"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $MUTATION_FP"
$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE UPDATE v = 2 WHERE 1 SETTINGS mutations_sync = 0" &
paused_pid=$!
wait_failpoint $MUTATION_FP
first=$($CLICKHOUSE_CLIENT --query "SELECT mutation_id $MUTATIONS ORDER BY mutation_id LIMIT 1")
$CLICKHOUSE_CLIENT --query "KILL MUTATION WHERE database = currentDatabase() AND table = '$TABLE' AND mutation_id = '$first' SYNC FORMAT Null"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $MUTATION_FP"
wait "$paused_pid"
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES $TABLE"
wait_for_query_result "SELECT count(), countIf(is_done) $MUTATIONS" "1	1"
$CLICKHOUSE_CLIENT --query "SELECT 'v after the second mutation:', v, count() FROM $TABLE GROUP BY v ORDER BY v"
