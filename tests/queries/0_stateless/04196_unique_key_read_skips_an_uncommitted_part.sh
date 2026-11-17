#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# UNIQUE KEY: a read takes the parts visible at its snapshot, not every active one. An INSERT's part is
# active from before its commit point, and the kill it puts on the row it replaces is not yet visible.
# Red if the overwritten key reads twice (`during` [1,2]) or the trivial count counts both rows.
# no-parallel: `unique_key_insert_pause_before_commit` is server-wide.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_unique_key 1"

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT unique_key_insert_pause_before_commit" 2>/dev/null || true
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_uncommitted"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE uk_uncommitted (k UInt64, v UInt64)
    ENGINE = MergeTree ORDER BY k UNIQUE KEY (k)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_uncommitted VALUES (1, 1)"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT unique_key_insert_pause_before_commit"
$CLICKHOUSE_CLIENT --async_insert 0 --query "INSERT INTO uk_uncommitted VALUES (1, 2)" &
insert_pid=$!
$CLICKHOUSE_CLIENT --max_execution_time 120 --query "
    SYSTEM WAIT FAILPOINT unique_key_insert_pause_before_commit PAUSE"

$CLICKHOUSE_CLIENT --query "SELECT 'during', groupArray(v) FROM uk_uncommitted WHERE k = 1"
$CLICKHOUSE_CLIENT --optimize_trivial_count_query 1 --query "SELECT 'during_count', count() FROM uk_uncommitted"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT unique_key_insert_pause_before_commit"
wait "$insert_pid"

$CLICKHOUSE_CLIENT --query "SELECT 'after', groupArray(v) FROM uk_uncommitted WHERE k = 1"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_uncommitted"
