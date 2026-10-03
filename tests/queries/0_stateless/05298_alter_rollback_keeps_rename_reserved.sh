#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel -- server-wide failpoints force and pause the alter's durable-rollback interval.
# no-replicated-database -- this exercises the non-replicated durable-rollback path directly.
# no-shared-merge-tree -- the retained block holder under test is `StorageMergeTree::alter`'s.

# While a failed rename mutation is un-registered but its metadata is not yet durably rolled back, its block
# must stay reserved: an update allocated above it must not commit before it, and a merge below it must not
# materialize that update's patch, or the update's version ends up below the merged part's and is skipped.
# Two arms: the durable rollback fails too (the rename is kept), or succeeds (the rename is undone and the
# waiting update still completes against the old column name).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_reservations.lib
. "$CUR_DIR"/mergetree_reservations.lib

THROW_AFTER_REGISTER_FP="mt_alter_throw_after_mutation_registered"
PAUSE_BEFORE_ROLLBACK_FP="mt_alter_pause_before_durable_rollback"
THROW_IN_ROLLBACK_FP="mt_alter_throw_in_durable_rollback"
TABLE="t_rename_reserved"
UPDATE_QUERY_ID_PREFIX="05298_rename_reserved_update_${CLICKHOUSE_DATABASE}"
MUTATIONS="FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE'"

function cleanup()
{
    for fp in $THROW_AFTER_REGISTER_FP $PAUSE_BEFORE_ROLLBACK_FP $THROW_IN_ROLLBACK_FP; do
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $fp"
    done
    wait
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null
}
trap cleanup EXIT

function run_arm()
{
    local throw_in_rollback=$1
    local update_query_id="${UPDATE_QUERY_ID_PREFIX}_${throw_in_rollback}"

    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS $TABLE SYNC;
        CREATE TABLE $TABLE (id UInt64, x String, y UInt64) ENGINE = MergeTree ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0;
        INSERT INTO $TABLE SELECT number, 'a', 1 FROM numbers(3);
        INSERT INTO $TABLE SELECT number + 3, 'b', 1 FROM numbers(3);
    "

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $THROW_AFTER_REGISTER_FP"
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $PAUSE_BEFORE_ROLLBACK_FP"
    if [[ "$throw_in_rollback" == "1" ]]; then
        $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $THROW_IN_ROLLBACK_FP"
    fi

    # The rename registers, then throws (first failpoint); the in-memory rollback un-registers it and
    # reaches the durable-rollback interval, where it pauses (second failpoint). With the third
    # failpoint the durable rollback throws too, so the server converges to the successful-alter
    # state: durable metadata keeps `z` and the rename mutation is re-registered. Without it the
    # rename is durably undone and its mutation file removed.
    $CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE RENAME COLUMN x TO z SETTINGS alter_sync = 2" > /dev/null 2>&1 &
    local alter_pid=$!
    wait_failpoint $PAUSE_BEFORE_ROLLBACK_FP

    echo "--- paused in the durable-rollback interval: the rename is un-registered but its block should still be reserved"
    $CLICKHOUSE_CLIENT --enable_lightweight_update 1 --query_id "$update_query_id" \
        --query "UPDATE $TABLE SET y = 2 WHERE 1" &
    local update_pid=$!

    # The update allocates its block and then waits for every non-update block below it. Without the
    # retained holder the rename's block is gone by now, the wait never starts, and this line never
    # appears; the update would commit straight away instead.
    local waiting=0
    for _ in $(seq 1 300); do
        $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
        waiting=$($CLICKHOUSE_CLIENT --query "
            SELECT count() FROM system.text_log
            WHERE event_date >= yesterday() AND query_id = '$update_query_id' AND message LIKE 'Waiting for committing blocks below%'")
        [[ "$waiting" -ge 1 ]] && break
        sleep 0.1
    done
    echo "the update is waiting behind the reservation: $((waiting >= 1))"
    local still_running
    still_running=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$update_query_id'")
    echo "the update waits behind the rename's reservation: $still_running"

    # Nothing is left to materialize above these two (still unmutated) source parts, so the merge
    # proceeds without recording a version past the reservation. Without the retained holder the
    # update's patch can commit before this point and the merge can materialize it, raising the merged
    # part's version above the rename's block.
    $CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $TABLE FINAL SETTINGS optimize_throw_if_noop = 1"
    echo "OPTIMIZE FINAL exit code: $?"

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $PAUSE_BEFORE_ROLLBACK_FP"
    wait "$alter_pid"
    wait "$update_pid"
    echo "update exit code: $?"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $THROW_AFTER_REGISTER_FP"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $THROW_IN_ROLLBACK_FP"

    wait_for_query_result "SELECT count() $MUTATIONS AND NOT is_done" "0"

    # Every row must read the update's value under the column name the alter ended with: the rename
    # (if kept) must still be applied to the merged part despite the interval, and the update must not
    # have been lost either way.
    $CLICKHOUSE_CLIENT --query "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = '$TABLE' AND name IN ('x', 'z')"
    if [[ "$throw_in_rollback" == "1" ]]; then
        $CLICKHOUSE_CLIENT --query "SELECT z, y, count() FROM $TABLE GROUP BY z, y ORDER BY z"
    else
        $CLICKHOUSE_CLIENT --query "SELECT x, y, count() FROM $TABLE GROUP BY x, y ORDER BY x"
    fi
}

echo "=== the durable rollback fails: the rename is kept"
run_arm 1
echo "=== the durable rollback succeeds: the rename is undone"
run_arm 0
