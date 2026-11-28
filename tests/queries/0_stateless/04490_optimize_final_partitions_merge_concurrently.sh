#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# - no-parallel: the test toggles the server-global failpoint `merge_task_projection_stage_pause`,
#   which would pause projection merges of other tests running at the same time.
# - no-fasttest: relies on failpoints, which are not available in the fast test build.

# This proves the behavior from issue #46770: OPTIMIZE TABLE ... FINAL on a non-replicated
# MergeTree table assigns and runs the merges of all partitions concurrently (the old sequential
# implementation would only ever have a single merge in flight). Merges are held in flight with a
# failpoint so the in-flight state is observed deterministically, without relying on timing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_optimize_concurrent SYNC"

# A projection makes every merge go through the projection-merge stage, which is where the
# failpoint pauses it. `max_bytes_to_merge_at_max_space_in_pool = 1` keeps ordinary *background*
# merges from selecting these parts (they exceed 1 byte), while OPTIMIZE ... FINAL still merges every
# part of a partition through the whole-partition path regardless of size. Otherwise a background
# merge for a different partition of the same table could also be paused by the failpoint and be
# counted below, letting the "more than one merge in flight" assertion pass even without OPTIMIZE
# assigning merges for multiple partitions at once.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_optimize_concurrent (p UInt8, k UInt64, v UInt64, PROJECTION agg (SELECT p, sum(v) GROUP BY p))
    ENGINE = MergeTree PARTITION BY p ORDER BY k
    SETTINGS optimize_on_insert = 0, max_bytes_to_merge_at_max_space_in_pool = 1, min_age_to_force_merge_seconds = 0"

# 4 partitions, several parts each.
for _ in 1 2 3; do
    $CLICKHOUSE_CLIENT --query "INSERT INTO t_optimize_concurrent SELECT number % 4, number, number FROM numbers(40000)"
done

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_optimize_concurrent SYNC" 2>/dev/null
}
trap cleanup EXIT

# Pause every merge at the projection stage, so all assigned merges stay in flight together.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT merge_task_projection_stage_pause"

# Assign the merges for all partitions at once; it will block on the paused merges.
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_optimize_concurrent FINAL" &
optimize_pid=$!

# Wait until more than one merge for this table is in flight at the same time.
observed_concurrent=0
for _ in {1..300}; do
    in_flight=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.merges
        WHERE database = currentDatabase() AND table = 't_optimize_concurrent'")
    if [[ "$in_flight" -gt "$observed_concurrent" ]]; then
        observed_concurrent=$in_flight
    fi
    if [[ "$observed_concurrent" -ge 2 ]]; then
        break
    fi
    sleep 0.1
done

# Old sequential OPTIMIZE FINAL would show at most one merge here; the new one assigns all at once.
if [[ "$observed_concurrent" -ge 2 ]]; then
    echo "multiple partition merges in flight: yes"
else
    echo "multiple partition merges in flight: no (observed $observed_concurrent)"
fi

# Let the merges finish and wait for OPTIMIZE to complete.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause"
wait "$optimize_pid"

# Every partition must be merged into a single part.
$CLICKHOUSE_CLIENT --query "
    SELECT max(parts_per_partition)
    FROM
    (
        SELECT count() AS parts_per_partition
        FROM system.parts
        WHERE database = currentDatabase() AND table = 't_optimize_concurrent' AND active
        GROUP BY partition
    )"
