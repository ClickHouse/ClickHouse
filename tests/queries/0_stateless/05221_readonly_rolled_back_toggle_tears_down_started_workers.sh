#!/usr/bin/env bash
# Tags: no-parallel
# The failpoint applies to settings changes across all tables.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# A fail point is server-global state: disarm every one this test enables on any path out of it,
# so that a paused `ALTER` is released and nothing fires in a concurrently running test.
function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_alter_settings_throw_before_metadata_commit" 2>/dev/null || true
}
trap cleanup EXIT

# Turning `table_readonly` back off starts the background workers before the metadata commit. When
# the commit then fails, the rollback must restore the worker lifecycle the table had before the
# ALTER, not only its settings: a table that was attached read-only has no `BackgroundJobsAssignee`
# scheduling tasks apart from the streaming one, which every table runs, so none may be left waking
# up on the still read-only table after the failure.
# The workers of a table that started writable and was made read-only later were already running
# before the failed ALTER and stay as they were.

assignees() {
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.background_schedule_pool
        WHERE database = currentDatabase() AND table = '$1' AND log_name LIKE 'BackgroundJobsAssignee:%'
          AND log_name != 'BackgroundJobsAssignee:Streaming'"
}
cleanup_threads() {
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.background_schedule_pool
        WHERE database = currentDatabase() AND table = '$1' AND log_name LIKE '%CleanupThread%'"
}

$CLICKHOUSE_CLIENT --multiquery -q "
    DROP TABLE IF EXISTS readonly_attached SYNC;
    DROP TABLE IF EXISTS readonly_made SYNC;
    CREATE TABLE readonly_attached (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS table_readonly = 1;
    DETACH TABLE readonly_attached;
    ATTACH TABLE readonly_attached;
    CREATE TABLE readonly_made (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS table_readonly = 0;
    INSERT INTO readonly_made SELECT number FROM numbers(10);
    ALTER TABLE readonly_made MODIFY SETTING table_readonly = 1;
"

echo "attached read-only, assignees before: $(assignees readonly_attached), cleanup threads before: $(cleanup_threads readonly_attached)"
made_before=$(assignees readonly_made)
echo "made read-only, has assignees before: $((made_before > 0))"

# Every worker is started and the commit itself fails.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_alter_settings_throw_before_metadata_commit"
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_attached MODIFY SETTING table_readonly = 0" 2>&1 \
    | grep -q -F 'FAULT_INJECTED' && echo 'attached: toggle failed at the commit: 1'
$CLICKHOUSE_CLIENT -q "INSERT INTO readonly_attached VALUES (1)" 2>&1 \
    | grep -q -F 'TABLE_IS_PERMANENTLY_READ_ONLY' && echo 'attached: still readonly after failed toggle: 1'
echo "attached: assignees after failed toggle: $(assignees readonly_attached), cleanup threads after failed toggle: $(cleanup_threads readonly_attached)"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_alter_settings_throw_before_metadata_commit"
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_made MODIFY SETTING table_readonly = 0" 2>&1 \
    | grep -q -F 'FAULT_INJECTED' && echo 'made: toggle failed at the commit: 1'
echo "made: assignees unchanged after failed toggle: $(( $(assignees readonly_made) == made_before ))"

# A retry completes the transition for both tables and the workers do run: a mutation can only be
# executed by the background operations assignee.
for table in readonly_attached readonly_made; do
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table MODIFY SETTING table_readonly = 0"
    echo "$table: assignees after retried toggle: $(( $(assignees $table) > 0 )), cleanup threads after retried toggle: $(cleanup_threads $table)"
    $CLICKHOUSE_CLIENT -q "INSERT INTO $table SELECT number + 100 FROM numbers(5)"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table DELETE WHERE k = 100 SETTINGS mutations_sync = 0"
    done_in_background=0
    for _ in $(seq 1 600); do
        if [[ "$($CLICKHOUSE_CLIENT -q "SELECT is_done FROM system.mutations
                    WHERE database = currentDatabase() AND table = '$table' ORDER BY create_time DESC LIMIT 1")" == "1" ]]; then
            done_in_background=1
            break
        fi
        sleep 0.1
    done
    echo "$table: background mutation executed after retried toggle: $done_in_background, rows: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM $table")"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE readonly_attached SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE readonly_made SYNC"
