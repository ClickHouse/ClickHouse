#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-ordinary-database, no-async-insert, no-parallel
# no-fasttest: transactions need the transaction log, which fasttest does not configure.
# no-replicated-database: executes ALTERs in a separate context, so transaction info is lost.
# no-ordinary-database: transactions require an Atomic database.
# no-async-insert: async inserts are not supported inside a transaction.
# no-parallel: enables a global pauseable failpoint on the mutation wait, so a concurrent test's
# mutation could consume the pause and let this test's ALTER run past it.

# A transactional mutation rewrites only the parts visible at its own snapshot: `selectPartsToMutate`
# skips the rest and never returns to them. A part another transaction commits after that snapshot, but
# numbered below the mutation version, is therefore never going to be rewritten, so removing it takes
# no obligation away and must be allowed. See #120902.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT storage_shared_merge_tree_mutate_pause_before_wait" ||:
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_dpum_txn SYNC" ||:
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_dpum_txn_ctl SYNC" ||:
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_dpum_txn_vis SYNC" ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_dpum_txn SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_dpum_txn_ctl SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_dpum_txn_vis SYNC"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_dpum_txn (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id"
# Both settings are pinned in the query because the stress runner passes them as client options and a
# transaction is rejected outright under either: apply_mutations_on_fly is not supported with
# transactions at all, and an async insert cannot take part in one. `< /dev/null` because a client
# INSERT ... VALUES otherwise waits on stdin.
$CLICKHOUSE_CLIENT --implicit_transaction=1 -q "INSERT INTO t_dpum_txn
    SETTINGS apply_mutations_on_fly = 0, async_insert = 0 VALUES (1, 100)" < /dev/null

# 01168_mutations_isolation.sh's ordering: tx B's part is committed after tx A's snapshot, so tx A's
# mutation can never rewrite it, while its block number stays below the mutation version.
# alter_update_mode is pinned because a lightweight update would carry the obligation in a patch part
# instead, which is a different guard. async_insert is pinned in the query rather than left to the
# no-async-insert tag, which sets a client option that these HTTP transaction sessions never see.
tx A "begin transaction"
tx B                      "begin transaction"
tx B                      "insert into t_dpum_txn settings async_insert = 0 values (2, 200)"
tx A "alter table t_dpum_txn update v = v + 1000 where 1 settings alter_update_mode = 'heavy'"
tx A "commit"
tx B                      "commit"

# The mutation rewrites the single visible part and then stays pending forever on the invisible one, so
# wait for that steady state instead of assuming it is already reached.
for _ in {1..300}
do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT countIf(NOT is_done AND parts_to_do = 1) FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_dpum_txn'")" = "1" ] && break
    sleep 0.1
done

echo "mutation armed $($CLICKHOUSE_CLIENT -q "SELECT is_done, parts_to_do FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_dpum_txn' AND NOT is_done")"
# 1100 is the visible part, rewritten; 200 is the invisible one the mutation will never rewrite.
echo "rows before detach"
$CLICKHOUSE_CLIENT -q "SELECT id, v FROM t_dpum_txn ORDER BY id SETTINGS apply_mutations_on_fly = 0"

P=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.parts WHERE database = currentDatabase()
    AND table = 't_dpum_txn' AND active AND min_block_number = 2")

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_dpum_txn DETACH PART '$P'" \
    > "$CLICKHOUSE_TMP"/05232_detach.txt 2>&1
if grep -q 'Code:' "$CLICKHOUSE_TMP"/05232_detach.txt; then echo "detach_invisible_refused 1"; else echo "detach_invisible_refused 0"; fi

# A plain pending mutation must still be refused, so the check above cannot pass with the guard removed
# altogether. It needs its own table: once the transactional mutation above has run, every part still
# visible to it has been rewritten, so none of them is owed anything any more.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_dpum_txn_ctl (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_dpum_txn_ctl"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_dpum_txn_ctl VALUES (1, 100)" < /dev/null
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_dpum_txn_ctl UPDATE v = v + 1000 WHERE 1
    SETTINGS mutations_sync = 0, alter_update_mode = 'heavy'"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_dpum_txn_ctl DETACH PART 'all_1_1_0'" \
    > "$CLICKHOUSE_TMP"/05232_control.txt 2>&1
if grep -q 'has not been applied to part' "$CLICKHOUSE_TMP"/05232_control.txt; then echo "control_refused_by_guard 1"; else echo "control_refused_by_guard 0"; fi

echo "detached parts $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.detached_parts
    WHERE database = currentDatabase() AND table = 't_dpum_txn'")"
echo "rows after detach"
$CLICKHOUSE_CLIENT -q "SELECT id, v FROM t_dpum_txn ORDER BY id SETTINGS apply_mutations_on_fly = 0"

# The mutation can now finish: its one outstanding part was one it was never going to rewrite.
for _ in {1..300}
do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT countIf(NOT is_done) FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_dpum_txn'")" = "0" ] && break
    sleep 0.1
done
echo "mutation pending after detach $($CLICKHOUSE_CLIENT -q "SELECT countIf(NOT is_done) FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_dpum_txn'")"

# The complement of the block above: a part that IS visible to a transactional mutation still owes it,
# so detaching that one must be refused. The state only exists while the ALTER is in flight, because
# `mutate` waits for the mutation unconditionally inside a transaction; the failpoint after
# `startMutation` is the seam 01168_mutations_isolation_3.sh's deadlock7 block uses to hold it there.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_dpum_txn_vis (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_dpum_txn_vis SETTINGS async_insert = 0 VALUES (1, 100)" < /dev/null
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_dpum_txn_vis"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT storage_shared_merge_tree_mutate_pause_before_wait"

tx C "begin transaction"
tx_async C "alter table t_dpum_txn_vis update v = v + 1000 where 1 settings alter_update_mode = 'heavy'" > /dev/null

for _ in {1..300}
do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_dpum_txn_vis'")" = "1" ] && break
    sleep 0.1
done

# Merges are stopped, so this state is steady rather than raced: the refusal below cannot pass
# vacuously against a part the mutation has already rewritten.
echo "txn_vis armed $($CLICKHOUSE_CLIENT -q "SELECT
    (SELECT countIf(NOT is_done) FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_dpum_txn_vis'),
    (SELECT countIf(active AND data_version = 1) FROM system.parts
        WHERE database = currentDatabase() AND table = 't_dpum_txn_vis' AND name = 'all_1_1_0')")"

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_dpum_txn_vis DETACH PART 'all_1_1_0'" \
    > "$CLICKHOUSE_TMP"/05232_txn_vis.txt 2>&1
if grep -q 'has not been applied to part' "$CLICKHOUSE_TMP"/05232_txn_vis.txt; then echo "detach_txn_vis_refused 1"; else echo "detach_txn_vis_refused 0"; fi

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT storage_shared_merge_tree_mutate_pause_before_wait"
$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_dpum_txn_vis"
tx_wait C
tx C "commit"

for _ in {1..300}
do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT countIf(NOT is_done) FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_dpum_txn_vis'")" = "0" ] && break
    sleep 0.1
done
echo "txn_vis mutation pending $($CLICKHOUSE_CLIENT -q "SELECT countIf(NOT is_done) FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_dpum_txn_vis'")"
echo "txn_vis sum $($CLICKHOUSE_CLIENT -q "SELECT sum(v) FROM t_dpum_txn_vis SETTINGS apply_mutations_on_fly = 0")"
