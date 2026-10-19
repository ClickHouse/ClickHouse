#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# SYSTEM SYNC MERGES returns only once a transaction started afterwards sees the merged part.
# A merge run inside a transaction makes its part active at once but visible to other transactions
# only from COMMIT.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib

set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS sync_merges_commit SYNC"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE sync_merges_commit (id UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS merge_selector_algorithm = 'Manual'"

$CLICKHOUSE_CLIENT --query "INSERT INTO sync_merges_commit SELECT number FROM numbers(10)"
$CLICKHOUSE_CLIENT --query "INSERT INTO sync_merges_commit SELECT number + 10 FROM numbers(10)"

tx 1 "BEGIN TRANSACTION"
tx 1 "OPTIMIZE TABLE sync_merges_commit FINAL"
$CLICKHOUSE_CLIENT --query "SYSTEM SCHEDULE MERGE sync_merges_commit PARTS 'all_1_1_0', 'all_2_2_0'"

# Uncommitted: the merged part is active, a new transaction still reads the two sources.
$CLICKHOUSE_CLIENT --query "
SELECT 'active_before_commit', arraySort(groupArray(name)) FROM system.parts
WHERE database = currentDatabase() AND table = 'sync_merges_commit' AND active"
$CLICKHOUSE_CLIENT --implicit_transaction 1 --query "SELECT 'read_before_commit', uniqExact(_part) FROM sync_merges_commit"

# Red if SYNC MERGES returns while a new transaction does not see the merged part (`returned`).
if $CLICKHOUSE_CLIENT --max_execution_time 2 --query "SYSTEM SYNC MERGES sync_merges_commit" 2>&1 | grep -q TIMEOUT_EXCEEDED; then
    echo -e "sync_merges_before_commit\ttimed_out"
else
    echo -e "sync_merges_before_commit\treturned"
fi

tx 1 "COMMIT"
$CLICKHOUSE_CLIENT --max_execution_time 120 --query "SYSTEM SYNC MERGES sync_merges_commit"
$CLICKHOUSE_CLIENT --implicit_transaction 1 --query "SELECT 'read_after_sync', uniqExact(_part), count() FROM sync_merges_commit"

$CLICKHOUSE_CLIENT --query "DROP TABLE sync_merges_commit SYNC"
