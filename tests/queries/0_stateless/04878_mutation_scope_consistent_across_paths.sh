#!/usr/bin/env bash
# Tags: no-replicated-database, no-ordinary-database, no-shared-merge-tree, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

# Every path that decides whether a mutation is finished must use the same scope. A part committed
# by another transaction above this mutation's snapshot is never its work, even though its block
# number is below the mutation's version: `system.mutations` reports the mutation done, so a waiter
# that scopes the same mutation differently once its transaction is gone would hang forever.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mut_scope SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_mut_scope (k UInt64, v String) ENGINE = MergeTree PARTITION BY k ORDER BY k"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mut_scope VALUES (1, 'a')"

tx 1 "BEGIN TRANSACTION" > /dev/null
tx 1 "INSERT INTO t_mut_scope SETTINGS async_insert = 0 VALUES (2, 'b')" > /dev/null

# tx2's snapshot is taken while tx1's part is still uncommitted, so that part stays invisible to it.
tx 2 "BEGIN TRANSACTION" > /dev/null
tx 2 "SELECT 'rows_visible_to_tx2', count() FROM t_mut_scope"

# Now tx1's part is committed with a CSN above tx2's snapshot, under a lower block number.
tx 1 "COMMIT" > /dev/null

# A mutation inside a transaction always waits, so this returns once the visible part is rewritten.
tx 2 "ALTER TABLE t_mut_scope UPDATE v = 'x' WHERE 1" > /dev/null
tx 2 "COMMIT" > /dev/null

$CLICKHOUSE_CLIENT -q "
    SELECT 'mutation_is_done', is_done
    FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_mut_scope'
"

# A barrier ALTER waits for that mutation from another session, with the transaction already gone.
# It must agree with the row above instead of waiting for a part the mutation never rewrites.
$CLICKHOUSE_CLIENT --max_execution_time 30 -q "ALTER TABLE t_mut_scope RENAME COLUMN v TO w" 2>&1 \
    | grep -oF "TIMEOUT_EXCEEDED" ||:

$CLICKHOUSE_CLIENT -q "
    SELECT 'renamed_columns', count()
    FROM system.columns
    WHERE database = currentDatabase() AND table = 't_mut_scope' AND name = 'w'
"

$CLICKHOUSE_CLIENT -q "SELECT k, w FROM t_mut_scope ORDER BY k"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_mut_scope SYNC"
