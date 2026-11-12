#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# - no-parallel: the test toggles the server-global failpoint `merge_task_projection_stage_pause`.
# - no-fasttest: failpoints are not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table-wide OPTIMIZE without PARTITION runs synchronously. It must reserve a merge-executor
# slot just like an explicit-partition OPTIMIZE, so it cannot bypass merge capacity or begin
# work during executor shutdown. Pause its projection merge and verify that the executor task
# metric rises together with the visible merge.
#
# Unlike an explicit-partition OPTIMIZE, a table-wide OPTIMIZE goes through the merge selector
# and respects `max_bytes_to_merge_at_max_space_in_pool`, so that setting cannot be used to keep
# background merges away. A huge `merge_selector_base` is used instead: background selection
# never reaches the required score, while an aggressive (OPTIMIZE-driven) selection overrides
# the base to 1 and picks the parts immediately.

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_optimize_without_partition_slot SYNC"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_optimize_without_partition_slot
    (
        k UInt64,
        v UInt64,
        PROJECTION agg (SELECT sum(v))
    )
    ENGINE = MergeTree
    ORDER BY k
    SETTINGS optimize_on_insert = 0, merge_selector_base = 1000"

$CLICKHOUSE_CLIENT --query "INSERT INTO t_optimize_without_partition_slot SELECT number, number FROM numbers(1000)"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_optimize_without_partition_slot SELECT number + 1000, number FROM numbers(1000)"

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_optimize_without_partition_slot SYNC" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT merge_task_projection_stage_pause"
$CLICKHOUSE_CLIENT --receive_timeout 900 --query "OPTIMIZE TABLE t_optimize_without_partition_slot" &
optimize_pid=$!

# `BackgroundMergesAndMutationsPoolTask` is server-global: unrelated background merges (e.g. of
# system tables) are charged to it as well, and their number changes while the test runs. Read the
# merge counts and the metric in a single snapshot and require the metric to account for every merge
# in flight. A baseline taken before the `OPTIMIZE` cannot be used: it also counts background tasks
# that are gone by the time the check runs, which makes the threshold unreachable.
reserved=no
deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
    state=$($CLICKHOUSE_CLIENT --query "
        SELECT
            (SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_optimize_without_partition_slot'),
            (SELECT count() FROM system.merges),
            (SELECT value FROM system.metrics WHERE metric = 'BackgroundMergesAndMutationsPoolTask')")
    in_flight=$(echo "$state" | cut -f1)
    total_in_flight=$(echo "$state" | cut -f2)
    tasks=$(echo "$state" | cut -f3)
    if [[ "$in_flight" -eq 1 && "$tasks" -ge "$total_in_flight" ]]; then
        reserved=yes
        break
    fi
    sleep 0.2
done

echo "table-wide optimize reserves merge slot: $reserved"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause"
wait "$optimize_pid"
