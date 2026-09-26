#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database
# no-parallel: uses a PAUSEABLE_ONCE failpoint that fires exactly once globally.
# no-shared-merge-tree: the failpoint is injected in ReplicatedMergeTreePartCheckThread; SharedMergeTree uses a different code path.
# no-replicated-database: uses explicit ReplicatedMergeTree ZooKeeper paths.
#
# Regression test for the cancel-vs-enqueue race in ReplicatedMergeTreePartCheckThread.
# cancelRemovedPartsCheck snapshots the in-range parts under parts_mutex, releases the lock to remove
# them from ZooKeeper, then re-locks and rechecks that parts_queue matches the snapshot. A foreground
# MOVE/REPLACE PARTITION installs only a drop-replace intent before calling cancelRemovedPartsCheck --
# the DROP_RANGE virtual part does not exist yet -- and enqueuePartForCheck consults only
# isGoingToBeDropped (virtual parts + drop parts), which ignores those intents. So a concurrent
# producer (here CHECK TABLE re-enqueueing a young part whose ZooKeeper node was removed) can add an
# in-range part into parts_queue during the lock gap. On re-lock the new entry has
# should_have_been_removed=true (it is in the drop range) but is_removed=false (it was not in the
# snapshot), so cancelRemovedPartsCheck throws LOGICAL_ERROR "Inconsistent parts_queue" (server abort
# in debug/sanitizer builds). This is a distinct throw from the cancel-vs-cancel count mismatch.
#
# We drive the race deterministically:
#   1. Seed a part into the check queue (part A): INSERT partition 0, remove its ZooKeeper node, then
#      CHECK TABLE. The part is younger than the (now removed) node, so the check thread re-enqueues
#      it for a delayed recheck -> it sits in parts_queue and gets snapshotted by the MOVE below.
#   2. Seed a second part in the same partition (part B) the same way, but do NOT CHECK it yet.
#   3. Start MOVE PARTITION 0 in the background; it enters cancelRemovedPartsCheck, snapshots {A},
#      and pauses in the lock gap (before the ZooKeeper removal), holding cancel_removed_parts_mutex.
#   4. Run CHECK TABLE for part B. It re-enqueues B via enqueuePart. Without the fix the enqueue takes
#      only parts_mutex and adds B while the MOVE is paused -> B is in parts_queue but not in the MOVE's
#      snapshot. With the fix enqueuePart blocks on cancel_removed_parts_mutex until the cancel finishes.
#   5. Resume the MOVE. Without the fix it re-locks, sees B (in drop range, not removed), and throws the
#      "Inconsistent parts_queue" logical error (server abort in debug/sanitizer, query exception in
#      release). With the fix the server stays alive; either query may still return a benign
#      CANNOT_ASSIGN_ALTER, which is fine -- the invariant under test is only that the server does not
#      abort with the parts_queue logical error.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ZK_PATH="/clickhouse/tables/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/cancel_enqueue_race"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT rmt_cancel_removed_parts_check_pause_in_gap" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS cancel_enqueue_race SYNC" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS cancel_enqueue_race_dst SYNC" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE cancel_enqueue_race (a UInt32, b UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/cancel_enqueue_race', 'r1')
    PARTITION BY a ORDER BY b;
    CREATE TABLE cancel_enqueue_race_dst (a UInt32, b UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/cancel_enqueue_race_dst', 'r1')
    PARTITION BY a ORDER BY b;
"

# --- Step 1: seed part A and put it into the check queue via CHECK TABLE ---
${CLICKHOUSE_CLIENT} -q "INSERT INTO cancel_enqueue_race VALUES (0, 1)"
PART_A=$(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'cancel_enqueue_race' AND active AND partition = '0' ORDER BY modification_time DESC LIMIT 1")
${CLICKHOUSE_KEEPER_CLIENT} -q "rm '${ZK_PATH}/replicas/r1/parts/${PART_A}'" >/dev/null 2>&1
${CLICKHOUSE_CLIENT} -q "CHECK TABLE cancel_enqueue_race SETTINGS check_query_single_value_result = 0" >/dev/null 2>&1 ||:

# --- Step 2: seed part B (same partition), orphan its ZooKeeper node, but do NOT check it yet ---
${CLICKHOUSE_CLIENT} -q "INSERT INTO cancel_enqueue_race VALUES (0, 2)"
PART_B=$(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'cancel_enqueue_race' AND active AND partition = '0' AND name != '${PART_A}' ORDER BY modification_time DESC LIMIT 1")
${CLICKHOUSE_KEEPER_CLIENT} -q "rm '${ZK_PATH}/replicas/r1/parts/${PART_B}'" >/dev/null 2>&1

# --- Step 3: pause the MOVE inside cancelRemovedPartsCheck's lock gap ---
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT rmt_cancel_removed_parts_check_pause_in_gap"

MOVE_OUT="${CLICKHOUSE_TMP}/cancel_enqueue_move.out"
CHECK_OUT="${CLICKHOUSE_TMP}/cancel_enqueue_check.out"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE cancel_enqueue_race MOVE PARTITION 0 TO TABLE cancel_enqueue_race_dst" >"$MOVE_OUT" 2>&1 &
MOVE_PID=$!

# Wait until the MOVE has snapshotted parts_to_remove ({A}) and paused in the gap.
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT rmt_cancel_removed_parts_check_pause_in_gap PAUSE"

# --- Step 4: while the MOVE is paused, re-enqueue part B via CHECK TABLE. Without the fix this adds B
#     into parts_queue (an in-range part not in the MOVE's snapshot); with the fix it blocks on
#     cancel_removed_parts_mutex, so it must run in the background or the test would deadlock. ---
${CLICKHOUSE_CLIENT} -q "CHECK TABLE cancel_enqueue_race SETTINGS check_query_single_value_result = 0" >"$CHECK_OUT" 2>&1 &
CHECK_PID=$!
sleep 2

# --- Step 5: resume the MOVE. Without the fix it re-locks, sees B in the drop range but not removed,
#     and hits the "Inconsistent parts_queue" logical error (server abort in debug/sanitizer). With the
#     fix neither query hits it. ---
${CLICKHOUSE_CLIENT} -q "SYSTEM NOTIFY FAILPOINT rmt_cancel_removed_parts_check_pause_in_gap"

wait $MOVE_PID 2>/dev/null ||:
wait $CHECK_PID 2>/dev/null ||:

# Fail if the parts_queue logical error surfaced (catches the release-build exception), or if the
# server aborted on it (debug/sanitizer) -- in that case the liveness probe below fails.
if grep -qF "Inconsistent parts_queue" "$MOVE_OUT" "$CHECK_OUT" 2>/dev/null; then
    echo "FAIL: Inconsistent parts_queue logical error (cancelRemovedPartsCheck cancel-vs-enqueue race)"
elif ${CLICKHOUSE_CLIENT} -q "SELECT 1" >/dev/null 2>&1; then
    echo "OK"
else
    echo "FAIL: server died (cancelRemovedPartsCheck cancel-vs-enqueue race)"
fi

rm -f "$MOVE_OUT" "$CHECK_OUT"
