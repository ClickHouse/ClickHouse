#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# UNIQUE KEY: a merge or mutation that starts right after its source part commits.
#   2. failed commit: a merge whose commit fails after it published its part can run again at once
# no-parallel: `unique_key_merge_fail_after_publish` is server-wide.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_unique_key 1"

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT unique_key_merge_fail_after_publish" 2>/dev/null || true
}
trap cleanup EXIT

# 2. failed commit: red if the rolled-back merge result keeps its name until the cleanup thread
# removes it, so the next OPTIMIZE fails with PART_IS_TEMPORARILY_LOCKED.
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP TABLE IF EXISTS uk_retry;
    CREATE TABLE uk_retry (id UInt64) ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
    SETTINGS merge_selector_algorithm = 'Manual';
    INSERT INTO uk_retry VALUES (1);
    INSERT INTO uk_retry VALUES (2);"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT unique_key_merge_fail_after_publish"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE uk_retry FINAL" 2>&1 | grep -o -m1 "FAULT_INJECTED"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE uk_retry FINAL"
$CLICKHOUSE_CLIENT --query "
    SELECT 'failed_commit_retried', groupArray(name), sum(rows) FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_retry' AND active"
$CLICKHOUSE_CLIENT --query "SELECT 'failed_commit_rows', groupArray(id) FROM (SELECT id FROM uk_retry ORDER BY id)"
$CLICKHOUSE_CLIENT --query "DROP TABLE uk_retry"
