#!/usr/bin/env bash
# Tags: no-replicated-database, no-parallel
# no-replicated-database: fails due to additional shard.
# no-parallel: enables a global pauseable failpoint on mutation registration, so a concurrent test
# could hit the pause instead of this test's own ALTER and make SYSTEM WAIT FAILPOINT return early.

# A DETACH must not be able to narrow the scope of a mutation that is published concurrently.
# Mutation publication holds the table's alter lock across the registration, and DETACH takes the
# same lock across its check and its commit, so the two cannot interleave. Without either half the
# DETACH runs its check before the mutation exists, succeeds, and the acknowledged UPDATE is
# silently never applied to the detached rows. See #120902.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_register_mutation" ||:
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_dpum_race SYNC" ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_dpum_race SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_dpum_race (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_dpum_race"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_dpum_race VALUES (1, 100), (2, 101)" < /dev/null

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_pause_before_register_mutation"

# Pauses inside startMutation, i.e. while the alter lock is held and before the mutation is
# registered: exactly the window a DETACH must not slip through. alter_update_mode here and
# apply_mutations_on_fly on the final read are pinned because the stress runner randomizes both as
# client options, and this test needs a heavy mutation that stays unapplied.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_dpum_race UPDATE v = v + 1000 WHERE 1
    SETTINGS mutations_sync = 0, alter_update_mode = 'heavy'" \
    > "$CLICKHOUSE_TMP"/05230_update.txt 2>&1 &

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_pause_before_register_mutation PAUSE"

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_dpum_race DETACH PART 'all_1_1_0'" \
    > "$CLICKHOUSE_TMP"/05230_detach.txt 2>&1 &

function detach_in_flight()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes
        WHERE current_database = currentDatabase() AND query LIKE 'ALTER TABLE t_dpum_race DETACH PART%'"
}

# The DETACH has to still be waiting while the mutation is unregistered. Without the lock it would
# instead run to completion here, so both "never showed up" and "already gone" are failures.
appeared=0
for _ in {1..300}
do
    if [ "$(detach_in_flight)" != "0" ]; then appeared=1; break; fi
    sleep 0.1
done

blocked=0
if [ "$appeared" = "1" ]
then
    sleep 2
    if [ "$(detach_in_flight)" != "0" ]; then blocked=1; fi
fi
echo "detach_blocked_while_mutation_unregistered $blocked"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_register_mutation"
wait

# The UPDATE must have gone through, and the DETACH must have been refused by the guard once it
# finally got the lock and could see the now-registered mutation.
if grep -q 'Code:' "$CLICKHOUSE_TMP"/05230_update.txt; then echo "update_failed 1"; else echo "update_failed 0"; fi
if grep -q 'has not been applied to part' "$CLICKHOUSE_TMP"/05230_detach.txt; then echo "detach_refused_by_guard 1"; else echo "detach_refused_by_guard 0"; fi

echo "part_still_active $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_dpum_race' AND active AND name = 'all_1_1_0'")"
echo "detached_parts $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_dpum_race'")"
echo "mutation_still_pending $($CLICKHOUSE_CLIENT -q "SELECT countIf(NOT is_done) FROM system.mutations WHERE database = currentDatabase() AND table = 't_dpum_race'")"
echo "sum_unchanged $($CLICKHOUSE_CLIENT -q "SELECT sum(v) FROM t_dpum_race SETTINGS apply_mutations_on_fly = 0")"
