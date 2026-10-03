#!/usr/bin/env bash
# Tags: no-parallel, no-shared-merge-tree, no-replicated-database
# no-parallel -- the pause failpoint is server-wide: it would hold any mutation started on any MergeTree table.
# no-shared-merge-tree -- the ordering under test is the in-memory mutation registration of StorageMergeTree.
# no-replicated-database -- a Replicated database runs DELETE through replicated DDL, not with the direct local timing assumed here.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_reservations.lib
. "$CUR_DIR"/mergetree_reservations.lib

function cleanup()
{
    # Disabling the failpoint also releases a mutation still paused at it, so the background client finishes.
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_pause_before_register_mutation" 2>/dev/null
    wait
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_mut_order SYNC" 2>/dev/null
}
trap cleanup EXIT

MUTATIONS="FROM system.mutations WHERE database = currentDatabase() AND table = 't_mut_order'"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_mut_order SYNC;
    CREATE TABLE t_mut_order (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_mut_order SELECT number FROM numbers(100);
"

# The first DELETE allocates its block number and writes mutation_N.txt, then pauses before registering the entry.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT mt_pause_before_register_mutation"
$CLICKHOUSE_CLIENT --lightweight_deletes_sync=0 --lightweight_delete_mode=alter_update --query "DELETE FROM t_mut_order WHERE id < 50" &
paused_pid=$!
wait_failpoint mt_pause_before_register_mutation

echo "--- while the first mutation is paused before registration"
# Nothing is registered yet: the paused mutation has a block number and a file, but no entry.
$CLICKHOUSE_CLIENT --query "SELECT count() $MUTATIONS"
# A second mutation must wait for the first one to register instead of registering a higher version first
# (which would move every part to the higher version and lose the first mutation). It waits on the alter lock,
# so with a short lock timeout it fails instead of overtaking.
$CLICKHOUSE_CLIENT --lightweight_deletes_sync=0 --lightweight_delete_mode=alter_update --lock_acquire_timeout=1 \
    --query "DELETE FROM t_mut_order WHERE id >= 50" 2>&1 | grep -m1 -o "TIMEOUT_EXCEEDED" || echo "the second DELETE did not wait"
$CLICKHOUSE_CLIENT --query "SELECT count() $MUTATIONS"

echo "--- after the first mutation is released"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_pause_before_register_mutation"
wait "$paused_pid"
wait_for_query_result "SELECT countIf(is_done) $MUTATIONS" "1"
# The second DELETE is retried now that the first one is registered.
$CLICKHOUSE_CLIENT --lightweight_deletes_sync=2 --lightweight_delete_mode=alter_update --query "DELETE FROM t_mut_order WHERE id >= 50"
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM t_mut_order;
    SELECT count(), countIf(is_done) $MUTATIONS;
    SELECT max(n) - min(n) + 1 = count() AS consecutive, min(is_done) AS all_done
    FROM (SELECT toUInt64(extract(mutation_id, '[0-9]+')) AS n, is_done $MUTATIONS);
"
