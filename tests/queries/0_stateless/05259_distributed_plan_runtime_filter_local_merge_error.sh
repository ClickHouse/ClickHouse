#!/usr/bin/env bash
# Tags: no-parallel, no-old-analyzer
# no-parallel: the fail points hit the runtime filter merges and receive branches of every query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `distributed_plan_execute_locally`, a failed runtime filter merge task (`rf_merge_*`)
# must fail the query, even when it fails after every data task has finished. The test pauses
# the merge task right before it finalizes the union. It releases the task once all data tasks
# have logged `QueryFinish`, and the task then throws.

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "
        SYSTEM DISABLE FAILPOINT distributed_plan_runtime_filter_merge_pause_before_finalize;
        SYSTEM DISABLE FAILPOINT distributed_plan_runtime_filter_merge_fails_before_finalize;
        SYSTEM DISABLE FAILPOINT distributed_plan_runtime_filter_receive_branch_fails_before_connect;
    " 2>/dev/null || true
    if [[ -n "${query_pid:-}" ]]
    then
        wait "$query_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
CREATE TABLE big (bid UInt64) ENGINE = MergeTree ORDER BY bid;
CREATE TABLE small (sid UInt64) ENGINE = MergeTree ORDER BY sid;
INSERT INTO big SELECT number FROM numbers(100000);
INSERT INTO small SELECT number * 100 FROM numbers(100);
"

# The bucket counts are pinned so that the filter has a single merge task.
QUERY="SELECT count() FROM big, small WHERE bid = sid SETTINGS
    enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0,
    make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0,
    distributed_plan_join_runtime_filters = 1,
    distributed_plan_default_reader_bucket_count = 4, distributed_plan_default_shuffle_join_bucket_count = 4,
    max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0"

# Every task of a local run logs under a `query_id` of its own, with the initiator's id as
# `initial_query_id`, the initiator's database, and the task id as the query text.
# Prints the finished data tasks, the finished merge tasks and the failed tasks.
function task_counts()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT
            countIf(type = 'QueryFinish' AND (query = 'main' OR startsWith(query, 'stage_'))),
            countIf(type = 'QueryFinish' AND startsWith(query, 'rf_merge_')),
            countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing'))
        FROM system.query_log
        WHERE event_date >= yesterday() AND initial_query_id IN (
            SELECT query_id FROM system.query_log
            WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '$1')"
}

# A run without fail points counts the data tasks to wait for and checks that there is exactly
# one merge task to pause.
warmup_query_id="${CLICKHOUSE_DATABASE}_warmup"
$CLICKHOUSE_CLIENT --query_id "$warmup_query_id" -q "$QUERY"
read -r data_tasks merge_tasks _ < <(task_counts "$warmup_query_id")
echo "merge tasks: $merge_tasks"
if [[ "$merge_tasks" != 1 ]]
then
    exit 1
fi

# The probe tasks do not wait for the filter. If they all finished before the merge task
# finalized the union, it would close its inputs without finalizing and never pause. So the test
# makes every receive branch fail before it connects. Such a branch never detaches from the
# filter stream, so the merge task always reaches the pause, and the filter fails open.
$CLICKHOUSE_CLIENT -q "
    SYSTEM ENABLE FAILPOINT distributed_plan_runtime_filter_receive_branch_fails_before_connect;
    SYSTEM ENABLE FAILPOINT distributed_plan_runtime_filter_merge_pause_before_finalize;
    SYSTEM ENABLE FAILPOINT distributed_plan_runtime_filter_merge_fails_before_finalize;
"

query_id="${CLICKHOUSE_DATABASE}_merge_failure"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "$QUERY" > "$CLICKHOUSE_TMP/$query_id.out" 2> "$CLICKHOUSE_TMP/$query_id.err" &
query_pid=$!

if ! timeout 120 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT distributed_plan_runtime_filter_merge_pause_before_finalize PAUSE"
then
    echo "the merge task did not pause"
    exit 1
fi

# Wait until every data task has finished. Also stop once a task has failed or the query has
# exited, so that a broken run prints its output instead of spinning here.
until read -r finished _ failed < <(task_counts "$query_id")
    [[ "$finished" -ge "$data_tasks" || "$failed" -gt 0 ]] || ! kill -0 "$query_pid" 2>/dev/null
do
    :
done
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT distributed_plan_runtime_filter_merge_pause_before_finalize"

status=0
wait "$query_pid" || status=$?
query_pid=
if [[ "$status" != 0 ]] && grep -q "Injected runtime filter merge failure" "$CLICKHOUSE_TMP/$query_id.err"
then
    echo "the query failed with the merge failure"
else
    echo "the query exited with status $status"
    cat "$CLICKHOUSE_TMP/$query_id.out" "$CLICKHOUSE_TMP/$query_id.err"
fi
rm -f "$CLICKHOUSE_TMP/$query_id.out" "$CLICKHOUSE_TMP/$query_id.err"
