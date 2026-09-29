#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel -- server-wide failpoints pause the next mutation registration and the next explicit `OPTIMIZE`.
# no-replicated-database -- the local timing of `ATTACH PART`, the mutation and `OPTIMIZE` is assumed.
# no-shared-merge-tree -- the bounds under test are `MergeTreeMergePredicate`'s.

# `ATTACH PART` of a patch takes a fresh block number without waiting, so it can land above a not yet registered
# mutation or above a running `OPTIMIZE`'s snapshot. The merge must leave such a patch for read time.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_reservations.lib
. "$CUR_DIR"/mergetree_reservations.lib

REGISTER_FP="mt_pause_before_register_mutation"
OPTIMIZE_FP="mt_optimize_pause_before_reading_patches"
MUTATION_QUERY_ID="05304_attached_patch_mutation_${CLICKHOUSE_DATABASE}"
OPTIMIZE_QUERY_ID="05304_attached_patch_optimize_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $REGISTER_FP"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $OPTIMIZE_FP"
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id IN ('$MUTATION_QUERY_ID', '$OPTIMIZE_QUERY_ID') ASYNC" >/dev/null 2>&1
    wait
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_patch_above_mutation SYNC" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_patch_above_snapshot SYNC" 2>/dev/null
}
trap cleanup EXIT

# Two regular parts and a detached patch part that updates the row of the first one.
function create_table_with_detached_patch()
{
    local table=$1
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS $table SYNC;
        CREATE TABLE $table (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, max_bytes_to_merge_at_max_space_in_pool = 1;
        INSERT INTO $table VALUES (1, 0);
        UPDATE $table SET a = 5 WHERE id = 1;
    "
    PATCH=$($CLICKHOUSE_CLIENT --query "
        SELECT name FROM system.parts
        WHERE database = currentDatabase() AND table = '$table' AND active AND startsWith(name, 'patch-')")
    $CLICKHOUSE_CLIENT --query "ALTER TABLE $table DETACH PART '$PATCH'"
    $CLICKHOUSE_CLIENT --query "INSERT INTO $table VALUES (2, 0)"
}

# A merge that applies a patch records the patch's version, so the merged part's data version tells.
function print_merged_part()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT 'merged part:', level, rows, data_version = min_block_number AS no_patch_applied
        FROM system.parts
        WHERE database = currentDatabase() AND table = '$1' AND active AND NOT startsWith(name, 'patch-')"
}

echo "--- patch above a mutation that is not registered yet"
create_table_with_detached_patch t_patch_above_mutation
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $REGISTER_FP"
$CLICKHOUSE_CLIENT --mutations_sync=0 --query_id "$MUTATION_QUERY_ID" \
    --query "ALTER TABLE t_patch_above_mutation UPDATE a = a + 100 WHERE 1" &
mutation_pid=$!
wait_failpoint $REGISTER_FP
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_patch_above_mutation ATTACH PART '$PATCH'"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_patch_above_mutation FINAL SETTINGS optimize_throw_if_noop = 1"
print_merged_part t_patch_above_mutation
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $REGISTER_FP"
wait "$mutation_pid"
wait_for_query_result "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_patch_above_mutation' AND NOT is_done" "0"
$CLICKHOUSE_CLIENT --query "SELECT id, a FROM t_patch_above_mutation ORDER BY id"

echo "--- patch attached after the OPTIMIZE snapshot"
create_table_with_detached_patch t_patch_above_snapshot
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $OPTIMIZE_FP"
$CLICKHOUSE_CLIENT --query_id "$OPTIMIZE_QUERY_ID" \
    --query "OPTIMIZE TABLE t_patch_above_snapshot FINAL SETTINGS optimize_throw_if_noop = 1" &
optimize_pid=$!
wait_failpoint $OPTIMIZE_FP
# Commits under the parts lock alone, while the paused selection holds the background mutex.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_patch_above_snapshot ATTACH PART '$PATCH'"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $OPTIMIZE_FP"
wait "$optimize_pid"
echo "OPTIMIZE FINAL exit code: $?"
print_merged_part t_patch_above_snapshot
$CLICKHOUSE_CLIENT --query "SELECT id, a FROM t_patch_above_snapshot ORDER BY id"
