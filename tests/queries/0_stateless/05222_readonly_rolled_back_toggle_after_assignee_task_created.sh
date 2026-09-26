#!/usr/bin/env bash
# Tags: no-parallel
# The failpoint applies to background jobs assignees of all tables.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# A fail point is server-global state: disarm every one this test enables on any path out of it,
# so that a paused `ALTER` is released and nothing fires in a concurrently running test.
function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_background_jobs_assignee_throw_after_task_created" 2>/dev/null || true
}
trap cleanup EXIT

# Turning `table_readonly` back off on a table that was attached read-only starts its background
# workers before the metadata commit. Starting an assignee allocates its scheduling task and then
# activates it, and the activation can throw after the task exists. The assignee must then be left
# exactly as it was, so that the rollback of the ALTER leaves the still read-only table without any
# `BackgroundJobsAssignee` task, and a retried ALTER must complete the transition.

worker_tasks() {
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.background_schedule_pool
        WHERE database = currentDatabase() AND table = 'readonly_task_created'
          AND (log_name LIKE 'BackgroundJobsAssignee:%' OR log_name LIKE '%CleanupThread%')
          AND log_name != 'BackgroundJobsAssignee:Streaming'"
}

$CLICKHOUSE_CLIENT --multiquery -q "
    DROP TABLE IF EXISTS readonly_task_created SYNC;
    CREATE TABLE readonly_task_created (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS table_readonly = 1;
    DETACH TABLE readonly_task_created;
    ATTACH TABLE readonly_task_created;
"
echo "worker tasks before: $(worker_tasks)"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_background_jobs_assignee_throw_after_task_created"
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_task_created MODIFY SETTING table_readonly = 0" 2>&1 \
    | grep -q -F 'FAULT_INJECTED' && echo 'toggle failed while activating an assignee task: 1'

$CLICKHOUSE_CLIENT -q "INSERT INTO readonly_task_created VALUES (1)" 2>&1 \
    | grep -q -F 'TABLE_IS_PERMANENTLY_READ_ONLY' && echo 'still readonly after failed toggle: 1'
echo "worker tasks after failed toggle: $(worker_tasks)"

# A retry completes the transition and the workers do run: a mutation can only be executed by the
# background operations assignee.
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_task_created MODIFY SETTING table_readonly = 0"
echo "has worker tasks after retried toggle: $(( $(worker_tasks) > 0 ))"
$CLICKHOUSE_CLIENT -q "INSERT INTO readonly_task_created SELECT number FROM numbers(5)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_task_created DELETE WHERE k = 0 SETTINGS mutations_sync = 0"
done_in_background=0
for _ in $(seq 1 600); do
    if [[ "$($CLICKHOUSE_CLIENT -q "SELECT is_done FROM system.mutations
                WHERE database = currentDatabase() AND table = 'readonly_task_created' ORDER BY create_time DESC LIMIT 1")" == "1" ]]; then
        done_in_background=1
        break
    fi
    sleep 0.1
done
echo "background mutation executed after retried toggle: $done_in_background, rows: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM readonly_task_created")"

$CLICKHOUSE_CLIENT -q "DROP TABLE readonly_task_created SYNC"
