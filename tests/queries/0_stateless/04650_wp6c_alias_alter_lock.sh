#!/usr/bin/env bash
# Tags: no-parallel
# - no-parallel: the pause failpoint is process-global

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_wp6c_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_wp6c_target"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_wp6c_target (a UInt64) ENGINE = MergeTree ORDER BY a"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_wp6c_alias ENGINE = Alias('$CLICKHOUSE_DATABASE', 't_wp6c_target')"

# Pause any MergeTree ALTER after it has taken the storage's alter_lock.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT alter_pause_in_mergetree_commit"

# T1: ALTER via the alias. It delegates to the target and, with the lock re-anchored to the target,
# pauses inside StorageMergeTree::alter while holding the TARGET's alter_lock.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_wp6c_alias ADD COLUMN x UInt64" &
T1_PID=$!

# Wait until T1 is actually paused at the failpoint (so it is holding the target's alter_lock).
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT alter_pause_in_mergetree_commit PAUSE"

# T2: a direct ALTER on the target must not acquire the target's alter_lock while the alias ALTER
# holds it -> it times out. Before the fix, the alias ALTER held only the alias's own lock, so this
# would acquire the target lock and race with T1, losing one of the two ADD COLUMNs.
$CLICKHOUSE_CLIENT --lock_acquire_timeout=1 -q "ALTER TABLE t_wp6c_target ADD COLUMN y UInt64" 2>&1 \
    | grep -qiE "DEADLOCK_AVOIDED|timed out" && echo "target locked" || echo "target NOT locked"

# Resume T1.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT alter_pause_in_mergetree_commit"
wait "$T1_PID"

# The alias ALTER applied its column; the racing direct ALTER failed, so its column is absent.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't_wp6c_target' AND name = 'x'"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't_wp6c_target' AND name = 'y'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_wp6c_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_wp6c_target"
