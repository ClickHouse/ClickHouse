#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# UNIQUE KEY: what lands between a merge's snapshot and its commit, parked there by failpoints.
#   1. self-kill: a late kill re-expressed on the merge result does not pin that result
#   2. right row: a late kill lands on the right merged row, sorted and unsorted
#   3. undetermined transaction: the merge and the DELETE refuse to run until it resolves, and the
#      INSERT that left it waits for the outcome
# no-parallel: `unique_key_merge_pause_before_commit` and `transaction_hold_unknown_state` are
# server-wide.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./parts.lib
. "$CURDIR"/parts.lib

set -e

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_unique_key 1"

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT unique_key_merge_pause_before_commit" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT transaction_force_unknown_state_after_commit" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT transaction_hold_unknown_state" 2>/dev/null || true
}
trap cleanup EXIT

# Prints 1 once `uk_late` has no empty active part and no inactive part left and part `$1` is gone, else 0.
parts_reclaimed() {
    if wait_for_delete_empty_parts uk_late "$CLICKHOUSE_DATABASE" 120 \
        && wait_for_delete_inactive_parts uk_late "$CLICKHOUSE_DATABASE" 120 \
        && [[ $($CLICKHOUSE_CLIENT --query "
            SELECT count() FROM system.parts
            WHERE database = currentDatabase() AND table = 'uk_late' AND name = '$1'") == 0 ]]; then
        echo 1
    else
        echo 0
    fi
}

# 1. self-kill: red if a part's bitmap for itself pins it against removal
# (`self_killing_part_reclaimed` 0).
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_late SYNC"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE uk_late (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
SETTINGS merge_selector_algorithm = 'Manual',
         min_bytes_for_wide_part = 0,
         old_parts_lifetime = 0,
         cleanup_delay_period = 1,
         max_cleanup_delay_period = 1,
         cleanup_delay_period_random_add = 0"

$CLICKHOUSE_CLIENT --query "INSERT INTO uk_late SELECT number, 'a' FROM numbers(0, 10)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_late SELECT number, 'b' FROM numbers(10, 10)"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT unique_key_merge_pause_before_commit"
$CLICKHOUSE_CLIENT --query "SYSTEM SCHEDULE MERGE uk_late PARTS 'all_1_1_0', 'all_2_2_0'"

# Wait for the merge thread to reach the pause, not for it to show in `system.merges`.
$CLICKHOUSE_CLIENT --max_execution_time 120 --query "
    SYSTEM WAIT FAILPOINT unique_key_merge_pause_before_commit PAUSE"

# Both sources are still active, so this commits after the merge's snapshot.
$CLICKHOUSE_CLIENT --query "DELETE FROM uk_late WHERE id < 3"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT unique_key_merge_pause_before_commit"
$CLICKHOUSE_CLIENT --max_execution_time 120 --query "SYSTEM SYNC MERGES uk_late"

$CLICKHOUSE_CLIENT --query "
    SELECT 'late_kill_on_the_result', name, unique_key_bitmap_versions FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_late' AND active AND name = 'all_1_2_1'"

# The marker's kills target the absorbed all_1_1_0, so it stops pinning on its own.
echo -e "marker_for_an_absorbed_target_gone\t$(parts_reclaimed all_3_3_0)"

$CLICKHOUSE_CLIENT --query "INSERT INTO uk_late SELECT number, 'c' FROM numbers(200, 5)"
$CLICKHOUSE_CLIENT --max_execution_time 120 --query "
    SYSTEM SCHEDULE MERGE uk_late PARTS 'all_1_2_1', 'all_4_4_0'"
$CLICKHOUSE_CLIENT --max_execution_time 120 --query "SYSTEM SYNC MERGES uk_late"

echo -e "self_killing_part_reclaimed\t$(parts_reclaimed all_1_2_1)"

$CLICKHOUSE_CLIENT --query "
    SELECT 'survivors', groupArray(id) FROM (SELECT id FROM uk_late ORDER BY id)"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_late SYNC"

# 2. right row: red if a late kill ignores where the merge sorted its row, or the offset of its
# source part in an unsorted merge (`interleaved` / `no_sorting_key`).
late_kill_lands_on_its_row() {
    local case=$1 order_by=$2 first=$3 second=$4 dead_at_snapshot=$5 late=$6
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_late_map SYNC"
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE uk_late_map (id UInt64, v String)
        ENGINE = MergeTree UNIQUE KEY (id) ORDER BY $order_by
        SETTINGS merge_selector_algorithm = 'Manual', min_bytes_for_wide_part = 0"
    $CLICKHOUSE_CLIENT --query "INSERT INTO uk_late_map SELECT id, 'a' FROM ($first)"
    $CLICKHOUSE_CLIENT --query "INSERT INTO uk_late_map SELECT id, 'b' FROM ($second)"
    $CLICKHOUSE_CLIENT --query "DELETE FROM uk_late_map WHERE id IN $dead_at_snapshot"

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT unique_key_merge_pause_before_commit"
    $CLICKHOUSE_CLIENT --query "SYSTEM SCHEDULE MERGE uk_late_map PARTS 'all_1_1_0', 'all_2_2_0'"
    $CLICKHOUSE_CLIENT --max_execution_time 120 --query "
        SYSTEM WAIT FAILPOINT unique_key_merge_pause_before_commit PAUSE"
    $CLICKHOUSE_CLIENT --query "DELETE FROM uk_late_map WHERE id IN $late"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT unique_key_merge_pause_before_commit"
    $CLICKHOUSE_CLIENT --max_execution_time 120 --query "SYSTEM SYNC MERGES uk_late_map"

    $CLICKHOUSE_CLIENT --query "
        SELECT '$case', groupArray(id) FROM (SELECT id FROM uk_late_map ORDER BY id)"
    $CLICKHOUSE_CLIENT --query "DROP TABLE uk_late_map SYNC"
}

EVENS="SELECT number * 2 AS id FROM numbers(10)"
ODDS="SELECT number * 2 + 1 AS id FROM numbers(10)"

# Evens in one part, odds in the other; every late kill follows a snapshot-dead row.
late_kill_lands_on_its_row interleaved "(id)" "$EVENS" "$ODDS" "(2, 5)" "(8, 9, 17)"

late_kill_lands_on_its_row no_sorting_key "tuple()" \
    "SELECT number AS id FROM numbers(10)" "SELECT number + 10 AS id FROM numbers(10)" "(1, 12)" "(5, 15)"

# 3. undetermined transaction: red if the merge or the DELETE runs while it is undetermined
# (`merge_refused` or `delete_refused` 0), or if the INSERT fails instead of waiting for the outcome
# (`insert_committed` 0).

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_merge_unknown_txn"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE uk_merge_unknown_txn (id UInt32, v UInt32)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
    SETTINGS min_bytes_for_wide_part = 0
"
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES uk_merge_unknown_txn"
for p in 0 1 2; do
    $CLICKHOUSE_CLIENT --query "
        INSERT INTO uk_merge_unknown_txn SELECT number + ${p} * 100 AS id, ${p} AS v FROM numbers(100)
    "
done

# Strand one transaction: `transaction_hold_unknown_state` keeps it from finalizing. Its INSERT logs
# the lost reply once the transaction is undetermined, then waits. Synchronous, so the log line comes
# back to this client.
INSERT_LOG="${CLICKHOUSE_TMP}/04173_stranded_insert.log"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT transaction_hold_unknown_state"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT transaction_force_unknown_state_after_commit"
$CLICKHOUSE_CLIENT --async_insert 0 --send_logs_level information \
    --query "INSERT INTO uk_merge_unknown_txn SELECT number + 500, 9 FROM numbers(10)" >/dev/null 2>"$INSERT_LOG" &
INSERT_PID=$!
for _ in {1..240}; do
    grep -q "Connection lost on attempt to commit transaction" "$INSERT_LOG" && break
    sleep 0.5
done
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT transaction_force_unknown_state_after_commit"

$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES uk_merge_unknown_txn"

$CLICKHOUSE_CLIENT --send_logs_level none --query "OPTIMIZE TABLE uk_merge_unknown_txn FINAL" 2>&1 \
    | grep -q "undetermined state" && echo "merge_refused 1" || echo "merge_refused 0"
$CLICKHOUSE_CLIENT --send_logs_level none --query "DELETE FROM uk_merge_unknown_txn WHERE id = 1" 2>&1 \
    | grep -q "undetermined state" && echo "delete_refused 1" || echo "delete_refused 0"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT transaction_hold_unknown_state"
wait "$INSERT_PID" && echo "insert_committed 1" || echo "insert_committed 0 ($(tail -1 "$INSERT_LOG"))"

$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE uk_merge_unknown_txn FINAL"
$CLICKHOUSE_CLIENT --query "
    SELECT count() = 310 AS all_rows_present FROM uk_merge_unknown_txn
"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_merge_unknown_txn"
