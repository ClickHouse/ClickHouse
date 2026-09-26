#!/usr/bin/env bash
# Tags: no-parallel
# The failpoint applies to `table_readonly` transitions of all tables.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# A fail point is server-global state: disarm every one this test enables on any path out of it,
# so that a paused `ALTER` is released and nothing fires in a concurrently running test.
function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_alter_readonly_throw_in_start_background_workers" 2>/dev/null || true
}
trap cleanup EXIT

# Turning `table_readonly` back off on a table that was attached read-only starts its background
# workers. If that start throws partway through, the table must stay read-only instead of becoming
# writable with some workers missing, and a retried ALTER must complete the transition.
# The start happens before the metadata commit, so the commit itself is the last step that can fail.

$CLICKHOUSE_CLIENT --multiquery -q "
    DROP TABLE IF EXISTS readonly_start_rollback SYNC;
    CREATE TABLE readonly_start_rollback (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS table_readonly = 0;
    INSERT INTO readonly_start_rollback SELECT number FROM numbers(10);
    ALTER TABLE readonly_start_rollback MODIFY SETTING table_readonly = 1;
    DETACH TABLE readonly_start_rollback;
    ATTACH TABLE readonly_start_rollback;
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_alter_readonly_throw_in_start_background_workers"
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_start_rollback MODIFY SETTING table_readonly = 0" 2>&1 \
    | grep -q -F 'FAULT_INJECTED' && echo 'toggle failed while starting workers: 1'

# The setting is rolled back in memory and on disk, so writes are still rejected.
$CLICKHOUSE_CLIENT -q "INSERT INTO readonly_start_rollback VALUES (100)" 2>&1 \
    | grep -q -F 'TABLE_IS_PERMANENTLY_READ_ONLY' && echo 'still readonly after failed toggle: 1'
$CLICKHOUSE_CLIENT -q "SELECT countSubstrings(create_table_query, 'table_readonly = 1') FROM system.tables
    WHERE database = currentDatabase() AND name = 'readonly_start_rollback'"

# The rollback restores the worker lifecycle of the read-only table: the assignee that was started
# before the failure is torn down again, and the cleanup thread is stopped as on a 0 -> 1 toggle.
$CLICKHOUSE_CLIENT -q "SELECT 'worker tasks after failed toggle: ' || toString(count()) FROM system.background_schedule_pool
    WHERE database = currentDatabase() AND table = 'readonly_start_rollback'
      AND (log_name LIKE 'BackgroundJobsAssignee:%' OR log_name LIKE '%CleanupThread%')
          AND log_name != 'BackgroundJobsAssignee:Streaming'"

# A retry completes the transition: the table is writable and every worker runs.
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_start_rollback MODIFY SETTING table_readonly = 0"
$CLICKHOUSE_CLIENT -q "SELECT 'worker tasks after retried toggle: ' || toString(count() >= 2) FROM system.background_schedule_pool
    WHERE database = currentDatabase() AND table = 'readonly_start_rollback'
      AND (log_name LIKE 'BackgroundJobsAssignee:%' OR log_name LIKE '%CleanupThread%')
          AND log_name != 'BackgroundJobsAssignee:Streaming'"
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_start_rollback DELETE WHERE k = 0 SETTINGS mutations_sync = 0"

done_in_background=0
for _ in $(seq 1 600); do
    if [[ "$($CLICKHOUSE_CLIENT -q "SELECT is_done FROM system.mutations
                WHERE database = currentDatabase() AND table = 'readonly_start_rollback'
                ORDER BY create_time DESC LIMIT 1")" == "1" ]]; then
        done_in_background=1
        break
    fi
    sleep 0.1
done
echo "background mutation executed after retried toggle: $done_in_background"
echo "rows: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_start_rollback')"

$CLICKHOUSE_CLIENT -q "DROP TABLE readonly_start_rollback SYNC"
