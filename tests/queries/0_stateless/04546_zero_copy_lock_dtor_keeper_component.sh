#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database, no-fasttest
# no-parallel: uses a failpoint that would intersect with concurrent tests
# no-fasttest: needs the s3 disk (minio) for zero-copy replication

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

# Always disable the failpoint on exit so an early failure (e.g. a WAIT FAILPOINT timeout)
# cannot leave it active and disrupt later tests. DISABLE on an inactive failpoint is a no-op.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_after_zero_copy_lock" 2>/dev/null || true' EXIT

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

# Drop the table while the mutation task still holds the zero-copy lock. DROP TABLE ... SYNC
# marks the paused task for deletion and then waits for it, so run it in the background (its output
# is irrelevant to the test); it blocks until we resume the failpoint below.
$CLICKHOUSE_CLIENT --query "DROP TABLE rmt SYNC" > /dev/null 2>&1 &
drop_pid=$!

# Wait until the DROP is actually in flight before resuming, so the executor has marked the paused
# task for deletion; then resuming tears it down (instead of letting it finalize and release the
# lock under a scoped component).
for _ in {1..300}; do
    running=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.processes
        WHERE current_database = currentDatabase() AND query LIKE 'DROP TABLE rmt SYNC%'")
    [[ "$running" -ge 1 ]] && break
    sleep 0.1
done

# Resume the mutation task. Because the storage is being dropped, the task is cancelled and
# destroyed on a background executor thread outside any Keeper component scope while it still
# holds the zero-copy lock, so ~ZooKeeperLock() releases the ephemeral lock without a component
# set. With enforce_keeper_component_tracking enabled this used to abort the server with
# "Current component is empty, please set it for your scope using Coordination::setCurrentComponent".
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_after_zero_copy_lock;"

# Do not let the (irrelevant) exit status of the background DROP abort the script under `set -e`.
wait "$drop_pid" || true

# The server must still be alive.
$CLICKHOUSE_CLIENT --query "SELECT 1;"
