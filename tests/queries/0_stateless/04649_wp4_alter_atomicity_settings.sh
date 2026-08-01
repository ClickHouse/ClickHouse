#!/usr/bin/env bash
# Tags: zookeeper, no-parallel
# - zookeeper: uses ReplicatedMergeTree
# - no-parallel: the failpoint is process-global, so a concurrent test's ALTER could also trip it

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_wp4_settings SYNC"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_wp4_settings (a UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_wp4_settings', 'r1')
    ORDER BY a"

# A settings-only and a comment-only ALTER take the non-replicated local-commit branches. Inject a
# failure right before the durable/coordinator commit: each ALTER must fail and leave the setting and
# comment unchanged, because they are now published only after the commit succeeds.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT atomic_db_fail_before_commit_alter_table"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_wp4_settings MODIFY SETTING max_replicated_merges_in_queue = 7" 2>&1 | grep -qF "FAULT_INJECTED" && echo 1 || echo 0
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_wp4_settings MODIFY COMMENT 'wp4'" 2>&1 | grep -qF "FAULT_INJECTED" && echo 1 || echo 0
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT atomic_db_fail_before_commit_alter_table"

# Neither the setting nor the comment was left applied.
$CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE t_wp4_settings" | grep -c -F "max_replicated_merges_in_queue = 7"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_wp4_settings' AND comment = 'wp4'"

# Without the failpoint both ALTERs succeed and are applied.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_wp4_settings MODIFY SETTING max_replicated_merges_in_queue = 7"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_wp4_settings MODIFY COMMENT 'wp4'"
$CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE t_wp4_settings" | grep -c -F "max_replicated_merges_in_queue = 7"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_wp4_settings' AND comment = 'wp4'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_wp4_settings SYNC"
