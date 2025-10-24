#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

set -e

counters_query="SELECT active_on_fly_data_mutations FROM system.tables WHERE database = currentDatabase() AND table = 't_mutations_counters'"

function wait_for_mutation_cleanup()
{
    for _ in {0..50}; do
        res=`$CLICKHOUSE_CLIENT --query "$counters_query"`
        if [[ $res == "0" ]]; then
            break
        fi
        sleep 0.5
    done
}

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_mutations_counters;

    CREATE TABLE t_mutations_counters (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;

    INSERT INTO t_mutations_counters VALUES (1, 2) (2, 3);

    SET mutations_sync = 0;
    SYSTEM STOP MERGES t_mutations_counters;

    ALTER TABLE t_mutations_counters UPDATE b = 100 WHERE a = 1;
    ALTER TABLE t_mutations_counters UPDATE b = 200 WHERE a = 2;

    $counters_query;
    SYSTEM START MERGES t_mutations_counters;
"

wait_for_mutation "t_mutations_counters" "mutation_3.txt"
wait_for_mutation_cleanup

$CLICKHOUSE_CLIENT --query "
    $counters_query;
    SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_mutations_counters' AND NOT is_done;
    SELECT * FROM t_mutations_counters ORDER BY a;

    SYSTEM STOP MERGES t_mutations_counters;
    ALTER TABLE t_mutations_counters UPDATE b = 1000 WHERE a = 1;

    $counters_query;
    KILL MUTATION WHERE mutation_id = 'mutation_4.txt' SYNC FORMAT Null;
    $counters_query;

    DROP TABLE t_mutations_counters;
"
