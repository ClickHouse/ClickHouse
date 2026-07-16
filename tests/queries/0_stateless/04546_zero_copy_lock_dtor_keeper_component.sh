#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database, no-fasttest
# no-parallel: uses failpoints that would intersect with concurrent tests
# no-fasttest: needs the s3 disk (minio) for zero-copy replication

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

# Always disable the failpoints on exit so an early failure (e.g. a WAIT FAILPOINT timeout)
# cannot leave them active and disrupt later tests. DISABLE on an inactive failpoint is a no-op.
trap '
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_after_zero_copy_lock" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_tree_background_task_marked_for_deletion" 2>/dev/null || true
' EXIT

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
# is taken), this failpoint fires only once tryCreateZeroCopyExclusiveLock() has succeeded, so
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
# which flags the task and then hits the failpoint above. Run it in the background (its output is
# irrelevant); it blocks until both failpoints are released.
$CLICKHOUSE_CLIENT --query "DROP TABLE rmt SYNC" > /dev/null 2>&1 &
drop_pid=$!

# Wait until the executor has flagged the task for deletion. After this, resuming the mutation task
# is guaranteed to tear it down while it still holds the zero-copy lock.
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT merge_tree_background_task_marked_for_deletion PAUSE;"

# Resume the mutation task. It is is_currently_deleting, so it is cancelled and destroyed on a
# background executor thread outside any Keeper component scope while it still holds the zero-copy
# lock, so ~ZooKeeperLock() releases the ephemeral lock. With enforce_keeper_component_tracking
# enabled this used to abort the server with
# "Current component is empty, please set it for your scope using Coordination::setCurrentComponent".
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_after_zero_copy_lock;"

# Let removeTasksCorrespondingToStorage proceed to wait for the (now destroyed) task, so DROP finishes.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_tree_background_task_marked_for_deletion;"

# Do not let the (irrelevant) exit status of the background DROP abort the script under `set -e`.
wait "$drop_pid" || true

# The server must still be alive.
$CLICKHOUSE_CLIENT --query "SELECT 1;"

# Second scope: the zero-copy part-move path. MergeTreeData::moveParts acquires/uses/releases a
# local ZeroCopyLock (tryCreateZeroCopyExclusiveLock does Keeper I/O), so it must run under a
# Keeper component too. A synchronous ALTER ... MOVE PART runs moveParts directly on the query
# thread, with no component set upstream, so with enforce_keeper_component_tracking enabled this
# used to abort the server with "Current component is empty ...".
$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0;

    CREATE TABLE mv (id UInt64, num UInt64)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/mv/', '1')
    ORDER BY id
    SETTINGS storage_policy = 'local_remote', allow_remote_fs_zero_copy_replication = 1;

    INSERT INTO mv VALUES (1, 1) (2, 2) (3, 3);

    ALTER TABLE mv MOVE PART 'all_0_0_0' TO DISK 's3_disk';

    DROP TABLE mv SYNC;
"

# The server must still be alive after the move.
$CLICKHOUSE_CLIENT --query "SELECT 1;"
