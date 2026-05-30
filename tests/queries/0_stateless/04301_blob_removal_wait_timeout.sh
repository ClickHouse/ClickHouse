#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-shared-merge-tree
# Tag rationale:
#   - no-fasttest: requires DiskObjectStorage (local_blob_storage) which is not in fast-test image.
#   - no-parallel: enables the server-wide `blob_killer_thread_pause_in_run` failpoint
#     which affects every `BlobKillerThread` instance on the server. Two tests running in
#     parallel would each enable and disable the same channel, defeating the synchronization.
#   - no-shared-merge-tree: inline `disk = disk(...)` SETTINGS are not supported on SMT.
#
# Regression test for PR #101680 — bounded wait in `DiskObjectStorageTransaction::commit`.
#
# Two contracts to verify:
#   1. Positive `wait_for_blob_removal_timeout_ms` (per-disk override of the server setting
#      `disk_object_storage_blob_removal_wait_timeout_ms`): `commit` must return when a
#      `BlobKillerThread` round is parked, leaving the blobs queued for asynchronous cleanup.
#   2. `wait_for_blob_removal_timeout_ms = 0`: `commit` must wait until the parked cleanup
#      is released, restoring the strict pre-fix semantics.
#
# Both directions are driven by the `blob_killer_thread_pause_in_run` PAUSEABLE failpoint
# added in `src/Disks/DiskObjectStorage/Replication/BlobKillerThread.cpp`. Enabling it
# blocks every call to `BlobKillerThread::run` at the top of the round, so `finished_rounds`
# never advances and a concurrent `waitRound` either hits the deadline (Test 1) or waits
# indefinitely until the failpoint is disabled (Test 2).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Defense-in-depth: always disable the failpoint on exit, even if the test fails
# halfway through. `SYSTEM DISABLE FAILPOINT` on an already-disabled failpoint is
# a no-op, so this is safe.
trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT blob_killer_thread_pause_in_run" 2>/dev/null || true' EXIT

# --- Test 1: positive timeout returns before the parked round finishes ---
#
# `wait_for_blob_removal_timeout_ms = 500` means `waitRound` must give up after 500 ms.
# With the failpoint enabled and the killer thread blocked at the top of `run`, the loop
# in `waitBlobRemoval` exits via the `triggerAndWait -> false` path after roughly one
# deadline. The whole DROP should comfortably fit within a few seconds even under
# sanitizer slowness; we assert it completes within 30 s (well below the would-be
# unbounded wait that triggers `Possible deadlock on shutdown` after 10 minutes).

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_timeout_short SYNC;
    CREATE TABLE t_timeout_short (a Int32) ENGINE = MergeTree() ORDER BY a
    SETTINGS disk = disk(
        type = 'object_storage',
        object_storage_type = 'local_blob_storage',
        metadata_type = 'local',
        path = '04301_short/',
        metadata_path = '04301_short_meta/',
        use_fake_transaction = 0,
        wait_for_blob_removal = 1,
        wait_for_blob_removal_timeout_ms = 500,
        persistent_removal_log = 1);
    INSERT INTO t_timeout_short VALUES (1), (2);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT blob_killer_thread_pause_in_run"

# DROP triggers `waitBlobRemoval`. The bounded loop must return without waiting
# for the parked round to complete.
if timeout 30 $CLICKHOUSE_CLIENT -q "DROP TABLE t_timeout_short SYNC"; then
    echo "Test 1 (timeout > 0): DROP returned before parked round"
else
    echo "Test 1 (timeout > 0): FAIL - DROP did not return within 30 s"
fi

# Re-arm the failpoint for Test 2. `DISABLE` removes the channel; `ENABLE` re-creates
# it with a fresh `pause_epoch`/`resume_epoch` pair.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT blob_killer_thread_pause_in_run"

# --- Test 2: timeout = 0 waits until the parked round is released ---
#
# `wait_for_blob_removal_timeout_ms = 0` restores the strict pre-fix semantics
# (`time_point::max` deadline). With the failpoint enabled, `waitRound` blocks
# indefinitely until we disable the failpoint, which wakes the paused killer
# thread, which completes the round and increments `finished_rounds`.

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_timeout_zero SYNC;
    CREATE TABLE t_timeout_zero (a Int32) ENGINE = MergeTree() ORDER BY a
    SETTINGS disk = disk(
        type = 'object_storage',
        object_storage_type = 'local_blob_storage',
        metadata_type = 'local',
        path = '04301_zero/',
        metadata_path = '04301_zero_meta/',
        use_fake_transaction = 0,
        wait_for_blob_removal = 1,
        wait_for_blob_removal_timeout_ms = 0,
        persistent_removal_log = 1);
    INSERT INTO t_timeout_zero VALUES (1), (2);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT blob_killer_thread_pause_in_run"

# Issue DROP in the background. With `timeout_ms = 0` it must block until we
# release the parked round.
$CLICKHOUSE_CLIENT -q "DROP TABLE t_timeout_zero SYNC" >/dev/null 2>&1 &
DROP_PID=$!

# Deterministically wait until the killer thread has actually reached and paused
# at the failpoint. After this returns, `waitRound` on the foreground DROP is
# guaranteed to be blocked (or will be, since `finished_rounds` cannot advance
# while the round is paused).
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT blob_killer_thread_pause_in_run PAUSE"

# Give the DROP a moment to actually enter `waitRound` (the
# `SYSTEM WAIT FAILPOINT ... PAUSE` only guarantees the killer is paused, not
# that the DROP has reached `waitBlobRemoval` yet) and then verify it is still
# running. With the strict semantics it must be.
sleep 1
if kill -0 "$DROP_PID" 2>/dev/null; then
    echo "Test 2 (timeout = 0): DROP still blocked while round is parked"
else
    echo "Test 2 (timeout = 0): FAIL - DROP returned before the round was released"
fi

# Release the parked round. The killer thread completes the round, increments
# `finished_rounds`, and the waiter inside `waitRound` returns true. The
# `waitBlobRemoval` loop re-checks `hasPendingRemovalBlobs`; once the cleanup
# is recorded, the loop exits and the DROP returns.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT blob_killer_thread_pause_in_run"

# Wait for the DROP to finish (with a generous bound to keep CI happy).
for _ in $(seq 1 60); do
    if ! kill -0 "$DROP_PID" 2>/dev/null; then
        break
    fi
    sleep 0.5
done

if kill -0 "$DROP_PID" 2>/dev/null; then
    echo "Test 2 (timeout = 0): FAIL - DROP did not finish within 30 s after release"
    # Avoid leaking the background process into later tests.
    kill -9 "$DROP_PID" 2>/dev/null || true
else
    echo "Test 2 (timeout = 0): DROP finished after release"
fi

# Reap the background process so the shell does not print a job-control message.
wait "$DROP_PID" 2>/dev/null || true
