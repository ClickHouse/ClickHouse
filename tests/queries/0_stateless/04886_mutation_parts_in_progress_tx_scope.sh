#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-ordinary-database, no-shared-merge-tree, no-fasttest, no-parallel-replicas
# Tag no-parallel: the failpoint is global

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib
# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

# A part committed above a transactional mutation's snapshot is not its work,
# so it must not show in that mutation's `parts_in_progress_names` while a later mutation rewrites it.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mut_in_progress SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_mut_in_progress (k UInt64, v String) ENGINE = MergeTree ORDER BY k"

tx 1 "BEGIN TRANSACTION" > /dev/null
tx 1 "INSERT INTO t_mut_in_progress SETTINGS async_insert = 0 VALUES (1, 'a')" > /dev/null
tx 2 "BEGIN TRANSACTION" > /dev/null
tx 2 "SELECT 'rows_visible_to_tx2', count() FROM t_mut_in_progress"
tx 1 "COMMIT" > /dev/null
tx 2 "ALTER TABLE t_mut_in_progress UPDATE v = 'x' WHERE 1" > /dev/null
tx 2 "COMMIT" > /dev/null

$CLICKHOUSE_CLIENT -q "
    SYSTEM ENABLE FAILPOINT mt_mutate_task_pause_in_prepare;
    ALTER TABLE t_mut_in_progress UPDATE v = 'y' WHERE 1;
"

wait_for_mutation_in_progress "t_mut_in_progress" "mutation_3.txt"

$CLICKHOUSE_CLIENT -q "
    SELECT mutation_id, parts_in_progress_names
    FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_mut_in_progress'
    ORDER BY mutation_id;
    SYSTEM DISABLE FAILPOINT mt_mutate_task_pause_in_prepare;
"

wait_for_mutation "t_mut_in_progress" "mutation_3.txt"

$CLICKHOUSE_CLIENT -q "SELECT k, v FROM t_mut_in_progress ORDER BY k"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_mut_in_progress SYNC"
