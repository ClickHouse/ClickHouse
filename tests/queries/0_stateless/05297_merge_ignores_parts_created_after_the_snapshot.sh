#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree, no-ordinary-database
# no-parallel -- server-wide failpoints pause the next mutation's block allocation, the next
# mutation registration, and the next explicit `OPTIMIZE`'s merge selection.
# no-replicated-database -- the local timing of `DELETE`, `INSERT` and `OPTIMIZE` is assumed.
# no-shared-merge-tree -- the candidate watermark under test is `MergeTreePartsCollector`'s.
# no-ordinary-database -- transactions need an Atomic database.

# A mutation that allocates its block after a merge selection copied its reservations is invisible to that
# snapshot, and so is a part activated afterwards. Merging the two older parts with that late part would
# leave the merged part at a low version, so the mutation would later rewrite data inserted after its own version.
# `MergeTreePartsCollector` drops parts with `min_block` above the snapshot watermark from the initial set of
# `OPTIMIZE FINAL`, so only the older parts merge. `optimize_throw_if_noop = 1` fails the test if the late part is refused instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_reservations.lib
. "$CUR_DIR"/mergetree_reservations.lib

MUTATION_FP="mt_mutation_pause_before_block_allocation"
REGISTER_FP="mt_pause_before_register_mutation"
OPTIMIZE_FP="mt_optimize_pause_after_reservation_snapshot"
TABLE="t_late_candidate"
MUTATIONS="FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE'"
DELETE_QUERY_ID="05297_late_candidate_delete_${CLICKHOUSE_DATABASE}"
OPTIMIZE_QUERY_ID="05297_late_candidate_optimize_${CLICKHOUSE_DATABASE}"
SRC="t_restored_src"
DST="t_restored_dst"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $MUTATION_FP"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $REGISTER_FP"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $OPTIMIZE_FP"
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id IN ('$DELETE_QUERY_ID', '$OPTIMIZE_QUERY_ID') ASYNC" >/dev/null 2>&1
    wait
    for table in $TABLE $SRC $DST; do
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $table SYNC" 2>/dev/null
    done
}
trap cleanup EXIT

# Runs the interleaving with the given `OPTIMIZE` query text, then prints what survived.
function run_late_part_arm()
{
    # `max_bytes_to_merge_at_max_space_in_pool = 1` keeps background merges away from these parts, so
    # the listing at the end shows what the `OPTIMIZE` and the mutation did; neither is limited by it.
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS $TABLE SYNC;
        CREATE TABLE $TABLE (id UInt64) ENGINE = MergeTree ORDER BY id
        SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;
        INSERT INTO $TABLE VALUES (1);
        INSERT INTO $TABLE VALUES (3);
    "

    # The DELETE finishes planning and parks right before it allocates its block number.
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $MUTATION_FP"
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $REGISTER_FP"
    $CLICKHOUSE_CLIENT --lightweight_deletes_sync=0 --lightweight_delete_mode=alter_update \
        --query_id "$DELETE_QUERY_ID" --query "DELETE FROM $TABLE WHERE id = 2" &
    local delete_pid=$!
    wait_failpoint $MUTATION_FP

    # The `OPTIMIZE` selection copies the reservations and the watermark (blocks 1 and 2 only) and parks
    # holding the background mutex.
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $OPTIMIZE_FP"
    $CLICKHOUSE_CLIENT --query_id "$OPTIMIZE_QUERY_ID" --query "$1" &
    local optimize_pid=$!
    wait_failpoint $OPTIMIZE_FP

    # The DELETE allocates its block above the watermark and parks before registering; waiting for that
    # pause orders the `INSERT` after the allocation. The inserted part lands above the DELETE's version.
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $MUTATION_FP"
    wait_failpoint $REGISTER_FP
    $CLICKHOUSE_CLIENT --query "INSERT INTO $TABLE VALUES (2)"

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $REGISTER_FP"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $OPTIMIZE_FP"
    wait "$optimize_pid"
    echo "OPTIMIZE FINAL exit code: $?"
    wait "$delete_pid"
    wait_for_query_result "SELECT count() $MUTATIONS AND NOT is_done" "0"

    # The row inserted after the mutation's version must survive. Parts are identified by their
    # block-number range, not by name: an unrelated later merge could rename them.
    $CLICKHOUSE_CLIENT --query "SELECT 'id=2 survives:', count() FROM $TABLE WHERE id = 2"
    $CLICKHOUSE_CLIENT --query "
        SELECT 'active parts:', min_block_number, max_block_number, level, rows
        FROM system.parts
        WHERE database = currentDatabase() AND table = '$TABLE' AND active
        ORDER BY min_block_number
    "
}

echo "--- OPTIMIZE FINAL"
run_late_part_arm "OPTIMIZE TABLE $TABLE FINAL SETTINGS optimize_throw_if_noop = 1"

# A transaction collects parts on its own path. The source parts are inserted before it begins:
# a transaction cannot merge its own parts.
echo "--- OPTIMIZE FINAL in a transaction"
run_late_part_arm "BEGIN TRANSACTION; OPTIMIZE TABLE $TABLE FINAL SETTINGS optimize_throw_if_noop = 1; COMMIT;"

# A restored part keeps its backed up mutation version, here above this table's block counter, while its
# block numbers are allocated here. Only `min_block` marks a late part, so it stays a merge candidate.
echo "--- a restored part with a foreign mutation version is merged"
BACKUP_NAME="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $SRC SYNC;
    CREATE TABLE $SRC (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;
    INSERT INTO $SRC SELECT number, 0 FROM numbers(3);
    ALTER TABLE $SRC UPDATE v = 1 WHERE 1 SETTINGS mutations_sync = 2;
    ALTER TABLE $SRC UPDATE v = 2 WHERE 1 SETTINGS mutations_sync = 2;
    ALTER TABLE $SRC UPDATE v = 3 WHERE 1 SETTINGS mutations_sync = 2;
    ALTER TABLE $SRC UPDATE v = 4 WHERE 1 SETTINGS mutations_sync = 2;
"
$CLICKHOUSE_CLIENT --query "BACKUP TABLE $SRC TO $BACKUP_NAME FORMAT Null"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $DST SYNC;
    CREATE TABLE $DST (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;
    INSERT INTO $DST VALUES (10, 10);
"
$CLICKHOUSE_CLIENT --query "RESTORE TABLE $SRC AS $DST FROM $BACKUP_NAME SETTINGS allow_non_empty_tables = 1 FORMAT Null"
$CLICKHOUSE_CLIENT --query "
    SELECT 'before:', min_block_number, max_block_number, data_version, rows,
        data_version > (SELECT max(max_block_number) FROM system.parts WHERE database = currentDatabase() AND table = '$DST')
    FROM system.parts
    WHERE database = currentDatabase() AND table = '$DST' AND active
    ORDER BY min_block_number
"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $DST FINAL SETTINGS optimize_throw_if_noop = 1"
echo "OPTIMIZE FINAL exit code: $?"
$CLICKHOUSE_CLIENT --query "
    SELECT 'after:', min_block_number, max_block_number, level, rows
    FROM system.parts
    WHERE database = currentDatabase() AND table = '$DST' AND active
    ORDER BY min_block_number
"
$CLICKHOUSE_CLIENT --query "SELECT id, v FROM $DST ORDER BY id"
