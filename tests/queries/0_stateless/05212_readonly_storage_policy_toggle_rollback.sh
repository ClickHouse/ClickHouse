#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: the failpoint applies to settings-only ALTERs of all tables.
# no-replicated-database, no-shared-merge-tree: `table_readonly` is a plain MergeTree setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# A single ALTER can turn `table_readonly` off and change the multi-volume `storage_policy`.
# `changeSettings` applies both before the metadata commit, and a policy change normally starts the
# background move assignee right away. For a table that started read-only, that assignee must stay
# disabled until the commit succeeds: otherwise it can observe the temporarily writable setting, queue
# a move, and run it after a failed commit has restored `table_readonly = 1`.
#
# `policy_05212_a` and `policy_05212_b` (tests/config/config.d/storage_conf_05212.xml) span the same
# volumes, so the switch can be rolled back; any other policy change is a one-way widening.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS readonly_policy_rollback SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE readonly_policy_rollback (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS storage_policy = 'policy_05212_a', table_readonly = 1"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_alter_settings_throw_before_metadata_commit"
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_policy_rollback
    MODIFY SETTING table_readonly = 0, storage_policy = 'policy_05212_b'" 2>&1 \
    | grep -q -F 'FAULT_INJECTED' && echo 'combined toggle failed before the commit: 1'
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_alter_settings_throw_before_metadata_commit"

# Both settings are rolled back: the table is still read-only and on its original policy.
$CLICKHOUSE_CLIENT -q "INSERT INTO readonly_policy_rollback VALUES (1)" 2>&1 \
    | grep -q -F 'TABLE_IS_PERMANENTLY_READ_ONLY' && echo 'still readonly after failed toggle: 1'
$CLICKHOUSE_CLIENT -q "SELECT 'policy after failed toggle: ' || storage_policy,
    'metadata: ' || toString(countSubstrings(create_table_query, 'table_readonly = 1')) || ' ' || toString(countSubstrings(create_table_query, 'policy_05212_b'))
    FROM system.tables WHERE database = currentDatabase() AND name = 'readonly_policy_rollback'"

# The failed ALTER did not enable the workers it started: no move was queued by the assignee that
# the `storage_policy` change would have woken up, and the cleanup thread is stopped again.
$CLICKHOUSE_CLIENT -q "SELECT 'moves after failed toggle: ' || toString(count()) FROM system.moves
    WHERE database = currentDatabase() AND table = 'readonly_policy_rollback'"
$CLICKHOUSE_CLIENT -q "SELECT 'cleanup thread after failed toggle: ' || toString(count()) FROM system.background_schedule_pool
    WHERE database = currentDatabase() AND table = 'readonly_policy_rollback' AND log_name LIKE '%CleanupThread%'"

# The retry completes both changes and starts every worker, including the move assignee.
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_policy_rollback
    MODIFY SETTING table_readonly = 0, storage_policy = 'policy_05212_b'"
$CLICKHOUSE_CLIENT -q "SELECT 'policy after retried toggle: ' || storage_policy
    FROM system.tables WHERE database = currentDatabase() AND name = 'readonly_policy_rollback'"
$CLICKHOUSE_CLIENT -q "SELECT 'move assignee after retried toggle: ' || toString(count()) FROM system.background_schedule_pool
    WHERE database = currentDatabase() AND table = 'readonly_policy_rollback' AND log_name = 'BackgroundJobsAssignee:Moving'"
$CLICKHOUSE_CLIENT -q "INSERT INTO readonly_policy_rollback VALUES (1)"
echo "rows: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_policy_rollback')"

$CLICKHOUSE_CLIENT -q "DROP TABLE readonly_policy_rollback SYNC"
