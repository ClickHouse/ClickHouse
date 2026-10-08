#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database, no-fasttest
# no-parallel: uses failpoints that would intersect with concurrent tests
# no-fasttest: needs the s3 disk (minio) for zero-copy replication

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# Always disable the failpoints on exit so an early failure (e.g. a WAIT FAILPOINT timeout)
# cannot leave them active and disrupt later tests. DISABLE on an inactive failpoint is a no-op.
trap '
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_after_zero_copy_lock" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_tree_background_task_marked_for_deletion" 2>/dev/null || true
' EXIT

# Remember when the test started so the final assertion only inspects log entries this test produced.
# When a zero-copy lock is released without a Keeper component under enforce_keeper_component_tracking,
# the "Current component is empty ..." LOGICAL_ERROR is caught and logged instead of aborting the
# server (~ZooKeeperLock swallows unlock exceptions, and the background moves executor swallows task
# exceptions). Checking server liveness therefore cannot distinguish the fixed and broken code; the
# text_log below can.
start_time=$($CLICKHOUSE_CLIENT --query "SELECT now64(6)")

$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0;

    CREATE TABLE rmt (id UInt64, num UInt64)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/rmt/', '1')
    ORDER BY id
    SETTINGS storage_policy = 's3_cache', allow_remote_fs_zero_copy_replication = 1;

    INSERT INTO rmt VALUES (1, 1) (2, 2) (3, 3);
"

# Pause the mutation task right AFTER it has acquired the zero-copy exclusive lock, and wait
# until a background thread is actually paused there. Unlike synchronizing on
# system.mutations.parts_in_progress_names (populated when the entry is queued, before the lock
# is taken), this failpoint fires only once tryCreateZeroCopyExclusiveLock has succeeded, so
# the task provably holds the lock and its ~ZooKeeperLock will have to release it.
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_mutate_task_pause_after_zero_copy_lock;
    ALTER TABLE rmt UPDATE num = num + 1 WHERE 1;
    SYSTEM WAIT FAILPOINT rmt_mutate_task_pause_after_zero_copy_lock PAUSE;
"

# Arm a second failpoint that fires inside MergeTreeBackgroundExecutor::removeTasksCorrespondingToStorage,
# right after the paused task is flagged is_currently_deleting = true. This is the deterministic
# point at which the task is guaranteed to take the destruction path (cancel + destroy via
# ~MutateFromLogEntryTask) when it resumes, instead of being requeued and finalized normally
# (which would release the lock under the executeStep component scope, not through the destructor).
# Waiting on DROP appearing in system.processes is not enough: the query can still be earlier in
# flushAndShutdown, before the task is marked for deletion.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT merge_tree_background_task_marked_for_deletion;"

# Drop the table while the mutation task still holds the zero-copy lock. DROP TABLE ... SYNC drives
# partialShutdown -> background_operations_assignee.finish -> removeTasksCorrespondingToStorage,
# which flags the task and then hits the failpoint above. Run it in the background (its stdout is
# irrelevant, but its exit status is checked below); it blocks until both failpoints are released.
$CLICKHOUSE_CLIENT --query "DROP TABLE rmt SYNC" > /dev/null 2>&1 &
drop_pid=$!

# Wait until the executor has flagged the task for deletion. After this, resuming the mutation task
# is guaranteed to tear it down while it still holds the zero-copy lock.
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT merge_tree_background_task_marked_for_deletion PAUSE;"

# Resume the mutation task. It is is_currently_deleting, so it is cancelled and destroyed on a
# background executor thread outside any Keeper component scope while it still holds the zero-copy
# lock, so ~ZooKeeperLock releases the ephemeral lock. With enforce_keeper_component_tracking
# enabled this used to abort the server with
# "Current component is empty, please set it for your scope using Coordination::setCurrentComponent".
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_after_zero_copy_lock;"

# Let removeTasksCorrespondingToStorage proceed to wait for the (now destroyed) task, so DROP finishes.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_tree_background_task_marked_for_deletion;"

# The background DROP must complete cleanly (its result is no longer discarded). If the mutation task
# teardown wedged the drop, `wait` returns non-zero and `set -e` fails the test here.
wait "$drop_pid"

# Second scope: the zero-copy part-move path. MergeTreeData::moveParts acquires/uses/releases a
# local ZeroCopyLock (tryCreateZeroCopyExclusiveLock does Keeper I/O), so it must run under a
# Keeper component too. A synchronous ALTER ... MOVE PART runs moveParts on the query thread under
# StorageReplicatedMergeTree::alter's existing component scope, so it never exercises the bug.
# alter_move_to_space_execute_async = 1 instead schedules the move on a background moves thread via
# ExecutableLambdaAdapter, with no component set upstream - exactly the path the new guard fixes.
$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0;

    CREATE TABLE mv (id UInt64, num UInt64)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/mv/', '1')
    ORDER BY id
    SETTINGS storage_policy = 'local_remote', allow_remote_fs_zero_copy_replication = 1;

    INSERT INTO mv VALUES (1, 1) (2, 2) (3, 3);

    SET alter_move_to_space_execute_async = 1;
    ALTER TABLE mv MOVE PART 'all_0_0_0' TO DISK 's3_disk';
"

# Wait until the background move has actually relocated the part to s3_disk. Without the
# MergeTreeData::moveParts guard the background task throws "Current component is empty" under
# enforce_keeper_component_tracking before the move can happen, so the part never lands on s3_disk
# and this loop times out (leaving the count below at 0, which fails the test).
for _ in {1..600}; do
    moved=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'mv' AND active AND disk_name = 's3_disk'")
    [ "$moved" = "1" ] && break
    sleep 0.1
done

# Assert the part moved to s3_disk (proves the guarded background moveParts path completed).
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'mv' AND active AND disk_name = 's3_disk';"

$CLICKHOUSE_CLIENT --query "DROP TABLE mv SYNC;"

# The observable that discriminates the fix from the bug: with the guard removed, both scopes above
# log "Current component is empty ..." (~ZooKeeperLock and the background moves executor catch the
# LOGICAL_ERROR instead of aborting). Assert that no such message was produced by this test.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log;"
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.text_log
    WHERE event_time_microseconds >= toDateTime64('$start_time', 6)
      AND message LIKE '%Current component is empty%';
"
