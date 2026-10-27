#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# - no-parallel: the test toggles the server-global failpoint `merge_task_projection_stage_pause`,
#   which would pause projection merges of other tests running at the same time.
# - no-fasttest: relies on failpoints, which are not available in the fast test build.

# OPTIMIZE TABLE ... FINAL reserves a background merge/mutate executor slot only after selecting a
# real merge. This test covers a set of partitions that are already merged with
# `optimize_skip_merged_partitions`, plus one partition with a real merge: no-op selections must
# not leave extra capacity charged in `BackgroundMergesAndMutationsPoolTask` while the real merge
# is in flight.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

pool_size=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'background_pool_size'")

# More partition candidates than worker threads, so the test also exercises a large batch of
# no-op selections while the one real merge remains paused.
num_partitions=$((pool_size + 2))

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
# second OPTIMIZE below skips all of these partitions as already merged. The data is deliberately
# tiny (a couple of rows per part): the test is about slot accounting, not about merge throughput,
# and the setup has to stay fast under sanitizers.
$CLICKHOUSE_CLIENT --query "INSERT INTO t_optimize_noop_release SELECT number % $num_partitions, number, number FROM numbers($((num_partitions * 2)))"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_optimize_noop_release SELECT number % $num_partitions, number, number FROM numbers($((num_partitions * 2)))"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_optimize_noop_release FINAL"

# Give exactly one partition something real to merge.
$CLICKHOUSE_CLIENT --query "INSERT INTO t_optimize_noop_release SELECT 0, number, number FROM numbers(2)"

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_optimize_noop_release SYNC" 2>/dev/null
}
trap cleanup EXIT

# Pause the one real merge at the projection stage, so it stays in flight while the no-op partition
# tasks resolve and the reservation drains down to it.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT merge_task_projection_stage_pause"

# The merge stays paused until the failpoint is disabled below, so this query is expected to run for
# as long as the checks take - give it a receive timeout well above the default, otherwise a slow
# sanitizer run makes the client give up on a query that is behaving exactly as intended.
$CLICKHOUSE_CLIENT --receive_timeout 900 --query "OPTIMIZE TABLE t_optimize_noop_release FINAL SETTINGS optimize_skip_merged_partitions = 1" &
optimize_pid=$!

# Wait for the real merge to appear and verify that only actually running merges are charged. The
# paused merge never completes on its own, so a no-op partition that retained a slot would keep the
# metric above the number of active merges and this loop would time out.
#
# `BackgroundMergesAndMutationsPoolTask` is server-global, so unrelated background merges (e.g. of
# system tables) are charged to it as well. Comparing it against the total number of merges in flight
# - instead of requiring the bare value 1 - makes the check independent of that noise.
released=no
for _ in {1..300}; do
    state=$($CLICKHOUSE_CLIENT --query "
        SELECT
            (SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_optimize_noop_release'),
            (SELECT count() FROM system.merges),
            (SELECT value FROM system.metrics WHERE metric = 'BackgroundMergesAndMutationsPoolTask')")
    in_flight=$(echo "$state" | cut -f1)
    total_in_flight=$(echo "$state" | cut -f2)
    reserved=$(echo "$state" | cut -f3)
    if [[ "$in_flight" -eq 1 && "$reserved" -le "$total_in_flight" ]]; then
        released=yes
        break
    fi
    sleep 0.2
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
