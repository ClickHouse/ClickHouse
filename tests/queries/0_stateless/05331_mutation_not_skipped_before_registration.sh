#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
# An UPDATE paused after taking its block number, before it is registered, is not skipped by a later mutation or by a merge.
# Red if v stays 0. no-parallel: global fail points; plain MergeTree only, the replicated paths register differently.

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

function mutation_first()
{
    wait_for_mutation "command LIKE '%DROP COLUMN%' AND (is_done OR notEmpty(parts_postpone_reasons))"
}

function merge_first()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads"
    $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t FINAL"
    $CLICKHOUSE_CLIENT -q "SELECT 'active parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active"
}

# $1: number of parts; $2: what runs while the UPDATE is paused.
function run_case()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE t (k UInt64, v UInt64, d UInt64) ENGINE = MergeTree ORDER BY k"
    for k in $(seq 1 "$1"); do $CLICKHOUSE_CLIENT -q "INSERT INTO t VALUES ($k, 0, 0)"; done

    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_pause_before_register_mutation"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE t UPDATE v = 1 WHERE 1 SETTINGS mutations_sync = 0" &
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_pause_before_register_mutation PAUSE"
    # Registered through the ALTER path, which does not pause, with a higher block number than the UPDATE.
    $CLICKHOUSE_CLIENT -q "ALTER TABLE t DROP COLUMN d SETTINGS alter_sync = 0"
    $2
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_register_mutation"
    wait
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads"
    wait_for_mutation "is_done AND command LIKE '%UPDATE%'"
    $CLICKHOUSE_CLIENT -q "SELECT k, v FROM t ORDER BY k"
}

echo "-- the DROP COLUMN mutation is selected first"
run_case 1 mutation_first
echo "-- a merge materializes the DROP COLUMN first"
run_case 2 merge_first

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
