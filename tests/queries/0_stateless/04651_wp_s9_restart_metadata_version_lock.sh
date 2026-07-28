#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree
# - zookeeper: uses ReplicatedMergeTree
# - no-parallel: the pause failpoint is process-global
# - no-shared-merge-tree: targets ReplicatedMergeTreeRestartingThread; under the SharedMergeTree test
#   mode the table becomes SharedMergeTree, SYSTEM RESTART REPLICA is rejected, the RMT failpoint never
#   fires, and SYSTEM WAIT FAILPOINT ... PAUSE would hang until the test timeout

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP=rmt_restarting_thread_pause_after_alter_lock

# Always disable the failpoint on exit, so an unexpected failure never leaves the restarting thread
# paused while holding alter_lock (which would contaminate later tests / stall shutdown).
cleanup() { $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" >/dev/null 2>&1; }
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_wp_s9 SYNC"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_wp_s9 (a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_wp_s9', 'r1')
    ORDER BY a"

# The restarting thread stamps metadata_version with an in-memory read-modify-write. It now takes the
# table's alter_lock around it; pause there (holding the lock).
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"

# Trigger the stamp path; it pauses while holding the alter_lock.
$CLICKHOUSE_CLIENT -q "SYSTEM RESTART REPLICA t_wp_s9" &
RESTART_PID=$!

# Bounded so a missed pause fails the test instead of hanging until the global timeout.
timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"

# A concurrent settings/comment ALTER must not acquire the alter_lock while the restarting thread holds
# it -> lockForAlter times out with DEADLOCK_AVOIDED. Before the fix the stamp took no lock, so this
# ALTER would run concurrently with the metadata_version stamp and could be reverted in memory.
$CLICKHOUSE_CLIENT --lock_acquire_timeout=1 -q "ALTER TABLE t_wp_s9 MODIFY COMMENT 'wp_s9'" 2>&1 \
    | grep -qF "DEADLOCK_AVOIDED" && echo "locked" || echo "not locked"

# Resume the restarting thread.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"
wait "$RESTART_PID"

# The table is functional after the restart: a comment ALTER now applies.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_wp_s9 MODIFY COMMENT 'after'"
$CLICKHOUSE_CLIENT -q "SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 't_wp_s9'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_wp_s9 SYNC"
