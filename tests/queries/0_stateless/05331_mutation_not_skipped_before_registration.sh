#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
# An `UPDATE` paused before registration blocks a later `ALTER` and is not skipped by a merge.
# no-parallel: global failpoints; plain `MergeTree` only, the replicated paths register differently.
# `mutate` holds the `ALTER` lock until registration, so a later `ALTER` times out on the lock.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_register_mutation"; $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads"' EXIT

function wait_for_mutation()
{
    for _ in {1..600}; do
        [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't' AND $1")" != "0" ] && return
        sleep 0.1
    done
    echo "timed out waiting for: $1"
}

function alter_times_out()
{
    # The `ALTER` cannot acquire the lock while the paused `UPDATE` holds it.
    $CLICKHOUSE_CLIENT --lock_acquire_timeout 1 -q "ALTER TABLE t DROP COLUMN d" 2>&1 | grep -m1 -o "DEADLOCK_AVOIDED" || echo "the ALTER did not wait for the alter lock"
}

function merge_first()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads"
    $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t FINAL"
    $CLICKHOUSE_CLIENT -q "SELECT 'active parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active"
}

# $1: number of parts; $2: what runs while the `UPDATE` is paused.
function run_case()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE t (k UInt64, v UInt64, d UInt64) ENGINE = MergeTree ORDER BY k"
    for k in $(seq 1 "$1"); do $CLICKHOUSE_CLIENT -q "INSERT INTO t VALUES ($k, 0, 0)"; done

    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_pause_before_register_mutation"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE t UPDATE v = 1 WHERE 1 SETTINGS mutations_sync = 0" &
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_pause_before_register_mutation PAUSE"
    $2
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_register_mutation"
    wait
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads"
    wait_for_mutation "is_done AND command LIKE '%UPDATE%'"
    $CLICKHOUSE_CLIENT -q "SELECT k, v FROM t ORDER BY k"
}

echo "-- the DROP COLUMN cannot register above the paused UPDATE"
run_case 1 alter_times_out
echo "-- a merge runs while the UPDATE is paused"
run_case 2 merge_first

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
