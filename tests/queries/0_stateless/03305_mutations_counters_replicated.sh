#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

set -e

value_before=`$CLICKHOUSE_CLIENT --query "SELECT value FROM system.metrics WHERE metric = 'ActiveDataMutations'"`

function wait_for_mutation_cleanup()
{
    for _ in {0..50}; do
        res=`$CLICKHOUSE_CLIENT --query "SELECT value FROM system.metrics WHERE metric = 'ActiveDataMutations'"`
        if [[ $res == "$value_before" ]]; then
            break
        fi
        sleep 0.5
    done
}

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_mutations_counters_2;

    CREATE TABLE t_mutations_counters_2 (a UInt64, b UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mutations_counters_2', '1') ORDER BY a;

    INSERT INTO t_mutations_counters_2 VALUES (1, 2) (2, 3);

    SET mutations_sync = 0;
    SYSTEM STOP MERGES t_mutations_counters_2;

    ALTER TABLE t_mutations_counters_2 UPDATE b = 100 WHERE a = 1;
    ALTER TABLE t_mutations_counters_2 UPDATE b = 200 WHERE a = 2;

    SYSTEM SYNC REPLICA t_mutations_counters_2 LIGHTWEIGHT;

    SELECT metric, value - $value_before FROM system.metrics WHERE metric = 'ActiveDataMutations';
    SYSTEM START MERGES t_mutations_counters_2;
"

wait_for_mutation "t_mutations_counters_2" "0000000001"
wait_for_mutation_cleanup

$CLICKHOUSE_CLIENT --query "
    SELECT metric, value - $value_before FROM system.metrics WHERE metric = 'ActiveDataMutations';
    SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_mutations_counters_2' AND NOT is_done;
    SELECT * FROM t_mutations_counters_2 ORDER BY a;

    SYSTEM STOP MERGES t_mutations_counters_2;
    ALTER TABLE t_mutations_counters_2 UPDATE b = 1000 WHERE a = 1;

    SYSTEM SYNC REPLICA t_mutations_counters_2 LIGHTWEIGHT;

    SELECT metric, value - $value_before FROM system.metrics WHERE metric = 'ActiveDataMutations';
    KILL MUTATION WHERE mutation_id = '0000000002' SYNC FORMAT Null;
    SELECT metric, value - $value_before FROM system.metrics WHERE metric = 'ActiveDataMutations';

    DROP TABLE t_mutations_counters_2;
"
