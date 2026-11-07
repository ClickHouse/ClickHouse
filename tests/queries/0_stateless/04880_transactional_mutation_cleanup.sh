#!/usr/bin/env bash
# Tags: no-replicated-database, no-ordinary-database, no-shared-merge-tree, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib
# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

# A finished mutation whose transaction has committed is as old as any other finished mutation:
# `finished_mutations_to_keep` must eventually drop its entry, and it must not block the cleanup
# of the finished entries behind it either.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_tx_mut_cleanup SYNC"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_tx_mut_cleanup (k UInt64, v String) ENGINE = MergeTree ORDER BY k
    SETTINGS finished_mutations_to_keep = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 0,
             cleanup_thread_preferred_points_per_iteration = 0"
# Held until both entries are listed below, so the background cleanup cannot race that listing.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP CLEANUP t_tx_mut_cleanup"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_tx_mut_cleanup VALUES (1, 'a')"

# A mutation inside a transaction waits for its parts to be rewritten; COMMIT then stamps its CSN.
tx 1 "BEGIN TRANSACTION" > /dev/null
tx 1 "ALTER TABLE t_tx_mut_cleanup UPDATE v = 'x' WHERE 1" > /dev/null
tx 1 "COMMIT" > /dev/null
wait_for_mutation "t_tx_mut_cleanup" "mutation_2.txt"

# A plain finished mutation behind the transactional one, so exactly one entry is over the limit.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_tx_mut_cleanup DELETE WHERE k = 0 SETTINGS mutations_sync = 1"
wait_for_mutation "t_tx_mut_cleanup" "mutation_3.txt"

$CLICKHOUSE_CLIENT -q "
    SELECT 'before_cleanup', mutation_id, is_done
    FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_tx_mut_cleanup'
    ORDER BY mutation_id"

# Release and wake the cleanup thread until the oldest finished entry - the transactional one - is gone.
for i in {1..30}
do
    $CLICKHOUSE_CLIENT -q "SYSTEM START CLEANUP t_tx_mut_cleanup"
    if [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_tx_mut_cleanup'") -eq 1 ]]; then
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo "Timed out while waiting for the finished transactional mutation to be cleaned up!"
    fi
    sleep 1
done

$CLICKHOUSE_CLIENT -q "
    SELECT 'after_cleanup', mutation_id, is_done
    FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_tx_mut_cleanup'
    ORDER BY mutation_id"

$CLICKHOUSE_CLIENT -q "SELECT k, v FROM t_tx_mut_cleanup ORDER BY k"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_tx_mut_cleanup SYNC"
