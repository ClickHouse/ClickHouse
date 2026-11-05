#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

set -e

function wait_for_mutation_cleanup()
{
    for _ in {0..50}; do
        res="$($CLICKHOUSE_CLIENT --query "SELECT active_on_fly_alter_mutations FROM system.tables WHERE database = currentDatabase() AND table = 't_mutations_counter_modify'")"
        if [[ $res == "0" ]]; then
            break
        fi
        sleep 0.5
    done
}

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_mutations_counter_modify;

    CREATE TABLE t_mutations_counter_modify (a UInt64, b UInt32) ENGINE = MergeTree ORDER BY a SETTINGS cleanup_delay_period = 1, cleanup_delay_period_random_add = 0, cleanup_thread_preferred_points_per_iteration = 0;

    INSERT INTO t_mutations_counter_modify VALUES (1, 2) (2, 3);

    SET alter_sync = 0;
    SET mutations_sync = 0;

    SYSTEM STOP MERGES t_mutations_counter_modify;
    ALTER TABLE t_mutations_counter_modify MODIFY COLUMN b String;

    SELECT 'active_on_fly_alter_mutations', active_on_fly_alter_mutations FROM system.tables WHERE database = currentDatabase() AND table = 't_mutations_counter_modify';

    SYSTEM START MERGES t_mutations_counter_modify;
"

wait_for_mutation "t_mutations_counter_modify" "mutation_2.txt"
wait_for_mutation_cleanup

$CLICKHOUSE_CLIENT --query "
    SELECT 'active_on_fly_alter_mutations', active_on_fly_alter_mutations FROM system.tables WHERE database = currentDatabase() AND table = 't_mutations_counter_modify';
    SELECT 'mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_mutations_counter_modify' AND NOT is_done;
    DROP TABLE t_mutations_counter_modify;
"
