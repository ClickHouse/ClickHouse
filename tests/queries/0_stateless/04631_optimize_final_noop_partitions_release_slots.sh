#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# - no-parallel: the test toggles the server-global failpoint `merge_task_projection_stage_pause`,
#   which would pause projection merges of other tests running at the same time.
# - no-fasttest: relies on failpoints, which are not available in the fast test build.

# OPTIMIZE TABLE ... FINAL reserves merge slots in the background merge/mutate executor before
# assigning the per-partition merges, and releases them as the partition tasks drain. This test
# covers the case where there are more partition candidates than reserved slots and almost all of
# them turn out to be no-ops (already merged + `optimize_skip_merged_partitions`), with only one
# real merge among them: once the no-op partitions resolve, the reservation must promptly drop to
# the number of merges actually running (here: 1), instead of keeping `background_pool_size` slots
# charged in `BackgroundMergesAndMutationsPoolTask` for the lifetime of the real merge.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

pool_size=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'background_pool_size'")

# More partition candidates than worker threads, so the reservation is capped below the number of
# partitions and the completed no-op tasks above that cap are the ones that must trigger releases.
num_partitions=$((pool_size * 2 + 2))

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_optimize_noop_release SYNC"

# A projection makes every merge go through the projection-merge stage, which is where the failpoint
# pauses it. `max_bytes_to_merge_at_max_space_in_pool = 1` stops *background* merges from selecting
# these parts (they exceed 1 byte), while OPTIMIZE ... FINAL still merges every part of a partition
# regardless of size - so the only merge in flight is the one this query assigns.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_optimize_noop_release (p UInt16, k UInt64, v UInt64, PROJECTION agg (SELECT p, sum(v) GROUP BY p))
    ENGINE = MergeTree PARTITION BY p ORDER BY k
    SETTINGS optimize_on_insert = 0, max_bytes_to_merge_at_max_space_in_pool = 1, min_age_to_force_merge_seconds = 0"

# Two parts per partition, then merge each partition into a single level >= 1 part, so that the
# second OPTIMIZE below skips all of these partitions as already merged.
$CLICKHOUSE_CLIENT --query "INSERT INTO t_optimize_noop_release SELECT number % $num_partitions, number, number FROM numbers($((num_partitions * 10)))"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_optimize_noop_release SELECT number % $num_partitions, number, number FROM numbers($((num_partitions * 10)))"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_optimize_noop_release FINAL"

# Give exactly one partition something real to merge.
$CLICKHOUSE_CLIENT --query "INSERT INTO t_optimize_noop_release SELECT 0, number, number FROM numbers(10)"

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_optimize_noop_release SYNC" 2>/dev/null
}
trap cleanup EXIT

# Pause the one real merge at the projection stage, so it stays in flight while the no-op partition
# tasks resolve and the reservation drains down to it.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT merge_task_projection_stage_pause"

$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_optimize_noop_release FINAL SETTINGS optimize_skip_merged_partitions = 1" &
optimize_pid=$!

# Wait for the real merge to appear, then for the reservation to drop to it. The paused merge never
# completes on its own, so if completed no-op partitions failed to release their slots, the metric
# would stay at the initial reservation (up to `background_pool_size`) forever and this loop would
# time out. Unrelated background merges (e.g. of system tables) can transiently bump the metric, so
# the loop waits for it to reach 1 rather than sampling it once.
released=no
for _ in {1..600}; do
    state=$($CLICKHOUSE_CLIENT --query "
        SELECT
            (SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_optimize_noop_release'),
            (SELECT value FROM system.metrics WHERE metric = 'BackgroundMergesAndMutationsPoolTask')")
    in_flight=$(echo "$state" | cut -f1)
    reserved=$(echo "$state" | cut -f2)
    if [[ "$in_flight" -eq 1 && "$reserved" -eq 1 ]]; then
        released=yes
        break
    fi
    sleep 0.1
done

echo "reservation drops to the single running merge: $released"

# Let the merge finish and wait for OPTIMIZE to complete.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause"
wait "$optimize_pid"

# Every partition must still be merged into a single part.
$CLICKHOUSE_CLIENT --query "
    SELECT max(parts_per_partition)
    FROM
    (
        SELECT count() AS parts_per_partition
        FROM system.parts
        WHERE database = currentDatabase() AND table = 't_optimize_noop_release' AND active
        GROUP BY partition
    )"
