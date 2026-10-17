#!/usr/bin/env bash
# Tags: no-parallel, no-async-insert, no-replicated-database, no-ordinary-database, no-shared-merge-tree, no-fasttest
# Tag no-parallel: the failpoint below is server-global

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

set -e

# `parts_postpone_reasons` must describe exactly the parts a mutation still has to rewrite, i.e. the
# same scope every completion check uses: blaming a part outside that scope points at work the
# mutation will never do, and dropping the reason of a part inside it hides the postpone entirely.

# Print the postponed part names and parts_to_do_names of the unfinished mutation, once a per-part
# reason is recorded ('all_parts' entries are unrelated pool-level postpones).
function wait_for_part_postpone_reasons()
{
    local table=$1
    local row=""
    for _ in $(seq 1 300); do
        row=$($CLICKHOUSE_CLIENT -q "
            SELECT arraySort(mapKeys(parts_postpone_reasons)), parts_to_do_names
            FROM system.mutations
            WHERE database = currentDatabase() AND table = '$table' AND NOT is_done
              AND notEmpty(parts_postpone_reasons) AND NOT mapContains(parts_postpone_reasons, 'all_parts')
        ")
        if [ -n "$row" ]; then
            echo "$row"
            return 0
        fi
        sleep 0.1
    done
    echo "Timed out waiting for parts_postpone_reasons of $table" >&2
    return 1
}

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_postpone_scope SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_postpone_scope (k UInt64, v String) ENGINE = MergeTree PARTITION BY k ORDER BY k"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_postpone_scope VALUES (1, 'a')"

tx 1 "BEGIN TRANSACTION" > /dev/null
tx 1 "INSERT INTO t_postpone_scope SETTINGS async_insert = 0 VALUES (2, 'b')" > /dev/null

# tx2's snapshot is taken while tx1's part is still uncommitted, so that part stays invisible to it.
tx 2 "BEGIN TRANSACTION" > /dev/null
tx 2 "SELECT count() FROM t_postpone_scope" > /dev/null

# Now tx1's part is committed above tx2's snapshot, under a block number below tx2's mutation.
tx 1 "COMMIT" > /dev/null

# The failpoint postpones every part, and a mutation inside a transaction waits, so run it async.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_max_part_size"
tx_async 2 "ALTER TABLE t_postpone_scope UPDATE v = 'x' WHERE 1" > /dev/null

printf 'outside_scope_part_not_blamed\t'
wait_for_part_postpone_reasons "t_postpone_scope"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_max_part_size"
tx_wait 2
tx 2 "COMMIT" > /dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE t_postpone_scope SYNC"

# A plain mutation above an uncommitted part has nothing countable to do yet, but it is not done and
# the reason recorded for that part is the only diagnostic the entry carries.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_postpone_pending SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_postpone_pending (k UInt64, v String) ENGINE = MergeTree PARTITION BY k ORDER BY k"

tx 3 "BEGIN TRANSACTION" > /dev/null
tx 3 "INSERT INTO t_postpone_pending SETTINGS async_insert = 0 VALUES (1, 'a')" > /dev/null

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_postpone_pending UPDATE v = 'x' WHERE 1 SETTINGS mutations_sync = 0"

printf 'pending_commit_part_still_blamed\t'
wait_for_part_postpone_reasons "t_postpone_pending"

tx 3 "COMMIT" > /dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE t_postpone_pending SYNC"
