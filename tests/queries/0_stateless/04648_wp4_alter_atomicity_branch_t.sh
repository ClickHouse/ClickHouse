#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database
# - zookeeper: uses ReplicatedMergeTree
# - no-parallel: the failpoint is process-global, so a concurrent test's ALTER could also trip it
# - no-replicated-database: a mixed structural + settings ALTER is prohibited on a Replicated database,
#   so this test targets the plain-Atomic branch-T path where settings/comment ride the structural CAS

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_wp4_branch_t SYNC"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_wp4_branch_t (a UInt64, b UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_wp4_branch_t', 'r1')
    ORDER BY a"

# A mixed structural + settings ALTER goes through branch T (the ZooKeeper-CAS retry loop).
# Inject a failure right before the metadata CAS: the ALTER must fail and, because settings/comment
# are now committed only after the CAS succeeds, leave BOTH the structural change AND the setting
# unchanged (a rejected ALTER is a no-op).
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT rmt_alter_fail_before_zk_multi"
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t_wp4_branch_t ADD COLUMN c UInt64, MODIFY SETTING max_replicated_merges_in_queue = 7
    " 2>&1 | grep -qF "FAULT_INJECTED" && echo 1 || echo 0
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT rmt_alter_fail_before_zk_multi"

# Column not added (structural part rejected).
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't_wp4_branch_t' AND name = 'c'"
# Setting not applied (settings part not left divergent).
$CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE t_wp4_branch_t" | grep -c -F "max_replicated_merges_in_queue = 7"

# Without the failpoint the same ALTER succeeds and applies both parts.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_wp4_branch_t ADD COLUMN c UInt64, MODIFY SETTING max_replicated_merges_in_queue = 7"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't_wp4_branch_t' AND name = 'c'"
$CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE t_wp4_branch_t" | grep -c -F "max_replicated_merges_in_queue = 7"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_wp4_branch_t SYNC"
