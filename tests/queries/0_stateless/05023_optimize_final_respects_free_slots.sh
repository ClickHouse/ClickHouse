#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# - no-parallel: the test toggles the server-global failpoint `merge_task_projection_stage_pause`.
# - no-fasttest: failpoints are not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Keep all but one merge-executor worker occupied. `OPTIMIZE FINAL` must then run exactly one
# partition merge at a time: every merge occupies a free executor slot, and a helper that cannot
# get a slot rolls its selection back (releasing the tagged parts and the reserved output space)
# before waiting - so the single free slot must still be picked up for one of the partitions
# instead of the whole statement stalling behind the occupied workers.
pool_size=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'background_pool_size'")
blockers=$((pool_size - 1))

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_optimize_final_free_slots SYNC"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_optimize_final_free_slots
    (
        p UInt16,
        k UInt64,
        v UInt64,
        PROJECTION agg (SELECT p, sum(v) GROUP BY p)
    )
    ENGINE = MergeTree PARTITION BY p ORDER BY k
    SETTINGS optimize_on_insert = 0, max_bytes_to_merge_at_max_space_in_pool = 1, min_age_to_force_merge_seconds = 0"

for ((partition = 0; partition <= blockers; ++partition)); do
    $CLICKHOUSE_CLIENT --query "INSERT INTO t_optimize_final_free_slots VALUES ($partition, 1, 1), ($partition, 2, 2)"
    $CLICKHOUSE_CLIENT --query "INSERT INTO t_optimize_final_free_slots VALUES ($partition, 3, 3), ($partition, 4, 4)"
done

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_optimize_final_free_slots SYNC" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT merge_task_projection_stage_pause"

# Occupy all but one executor workers with explicit partition merges.
for ((partition = 0; partition < blockers; ++partition)); do
    $CLICKHOUSE_CLIENT --receive_timeout 900 --query "OPTIMIZE TABLE t_optimize_final_free_slots PARTITION ID '$partition' FINAL" &
done

for _ in {1..300}; do
    active_blockers=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_optimize_final_free_slots'")
    [[ "$active_blockers" -eq "$blockers" ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --receive_timeout 900 --query "OPTIMIZE TABLE t_optimize_final_free_slots FINAL" &

# Exactly one remaining worker can execute a target merge. The helpers of the other target
# partitions wait without retaining merge tags, disk reservations, or executor slots.
for _ in {1..300}; do
    active_target=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_optimize_final_free_slots'")
    [[ "$active_target" -eq "$pool_size" ]] && break
    sleep 0.1
done

if [[ "$active_target" -eq "$pool_size" ]]; then
    echo "optimize final uses the one free merge slot: yes"
else
    echo "optimize final uses the one free merge slot: no"
fi

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause"
wait
