#!/usr/bin/env bash
# Tags: zookeeper, no-parallel
# - zookeeper: uses ReplicatedMergeTree
# - no-parallel: the pause failpoint is process-global

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_wp_s9 SYNC"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_wp_s9 (a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_wp_s9', 'r1')
    ORDER BY a"

# The restarting thread stamps metadata_version with an in-memory read-modify-write. It now takes the
# table's alter_lock around it; pause there (holding the lock).
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT rmt_restarting_thread_pause_after_alter_lock"

# Trigger the stamp path; it pauses while holding the alter_lock.
$CLICKHOUSE_CLIENT -q "SYSTEM RESTART REPLICA t_wp_s9" &
RESTART_PID=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT rmt_restarting_thread_pause_after_alter_lock PAUSE"

# A concurrent settings/comment ALTER must not acquire the alter_lock while the restarting thread holds
# it -> it times out. Before the fix the stamp took no lock, so this ALTER would run concurrently with
# the metadata_version stamp and could be reverted in memory.
$CLICKHOUSE_CLIENT --lock_acquire_timeout=1 -q "ALTER TABLE t_wp_s9 MODIFY COMMENT 'wp_s9'" 2>&1 \
    | grep -qiE "DEADLOCK_AVOIDED|timed out" && echo "locked" || echo "not locked"

# Resume the restarting thread.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT rmt_restarting_thread_pause_after_alter_lock"
wait "$RESTART_PID"

# The table is functional after the restart: a comment ALTER now applies.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_wp_s9 MODIFY COMMENT 'after'"
$CLICKHOUSE_CLIENT -q "SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 't_wp_s9'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_wp_s9 SYNC"
