#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-object-storage-with-slow-build
# - no-parallel: the test toggles the server-global failpoint `merge_task_projection_stage_pause`,
#   which would pause projection merges of other tests running at the same time.
# - no-fasttest: relies on failpoints, which are not available in the fast test build.
# - no-object-storage-with-slow-build: the table has one part per partition per INSERT across many
#   partitions, each part carrying a projection. On object storage with a sanitizer build, writing
#   all of those parts is slow enough to push the test past the per-test time limit (it takes ~100s
#   even when it passes). The property under test - that OPTIMIZE FINAL bounds the number of
#   concurrent partition merges by the worker budget - does not depend on the storage backend, so
#   skipping this slow combination loses no coverage.

# OPTIMIZE TABLE ... FINAL on a non-replicated MergeTree table assigns and runs the merges of all
# partitions in parallel (issue #46770). The foreground parallelism must stay within the operator's
# configured merge worker budget (`background_pool_size`), not the larger task-slot count
# (`background_pool_size` * `background_merges_mutations_concurrency_ratio`): each partition merge
# runs to completion on its own thread, so running one merge per task slot would use up to the
# ratio times the merge worker parallelism the operator configured. This test uses more partitions
# than worker threads and asserts that at most `background_pool_size` partition merges are in flight
# at once.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

pool_size=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'background_pool_size'")

# More partitions than worker threads (and, for any ratio >= 1, than the task-slot count too), so a
# build sizing the local pool from the free task-slot count would run more than `background_pool_size`
# merges at once, while the fixed one caps at `background_pool_size`.
num_partitions=$((pool_size * 2 + 2))

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_optimize_worker_budget SYNC"

# A projection makes every merge go through the projection-merge stage, which is where the failpoint
# pauses it. `max_bytes_to_merge_at_max_space_in_pool = 1` stops *background* merges from selecting
# these parts (they exceed 1 byte), while OPTIMIZE ... FINAL still merges every part of a partition
# regardless of size - so the only merges in flight are the ones this query assigns.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_optimize_worker_budget (p UInt16, k UInt64, v UInt64, PROJECTION agg (SELECT p, sum(v) GROUP BY p))
    ENGINE = MergeTree PARTITION BY p ORDER BY k
    SETTINGS optimize_on_insert = 0, max_bytes_to_merge_at_max_space_in_pool = 1, min_age_to_force_merge_seconds = 0"

# Two parts per partition, so every partition has something to merge.
$CLICKHOUSE_CLIENT --query "INSERT INTO t_optimize_worker_budget SELECT number % $num_partitions, number, number FROM numbers($((num_partitions * 50)))"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_optimize_worker_budget SELECT number % $num_partitions, number, number FROM numbers($((num_partitions * 50)))"

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_optimize_worker_budget SYNC" 2>/dev/null
}
trap cleanup EXIT

# Pause every merge at the projection stage, so all started merges stay in flight together.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT merge_task_projection_stage_pause"

# Assign the merges for all partitions at once; it blocks on the paused merges.
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_optimize_worker_budget FINAL" &
optimize_pid=$!

# The paused merges never complete on their own, so the in-flight count only grows until the pool is
# full and then stays constant. Wait for it to stabilise and record the peak.
peak=0
prev=-1
stable=0
for _ in {1..600}; do
    in_flight=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.merges
        WHERE database = currentDatabase() AND table = 't_optimize_worker_budget'")
    if [[ "$in_flight" -gt "$peak" ]]; then
        peak=$in_flight
    fi
    if [[ "$in_flight" -eq "$prev" && "$in_flight" -gt 0 ]]; then
        stable=$((stable + 1))
    else
        stable=0
    fi
    prev=$in_flight
    if [[ "$stable" -ge 20 ]]; then
        break
    fi
    sleep 0.1
done

# With the fix the peak stays bounded by background_pool_size even though there are more partitions;
# a build that sized the pool from the task-slot count would show up to
# background_pool_size * background_merges_mutations_concurrency_ratio merges here.
if [[ "$peak" -gt 1 && "$peak" -le "$pool_size" ]]; then
    echo "concurrent partition merges within background_pool_size: yes"
else
    echo "concurrent partition merges within background_pool_size: no (peak=$peak, background_pool_size=$pool_size)"
fi

# Let the merges finish and wait for OPTIMIZE to complete.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause"
wait "$optimize_pid"

# Every partition must still be merged into a single part.
$CLICKHOUSE_CLIENT --query "
    SELECT max(parts_per_partition)
    FROM
    (
        SELECT count() AS parts_per_partition
        FROM system.parts
        WHERE database = currentDatabase() AND table = 't_optimize_worker_budget' AND active
        GROUP BY partition
    )"
