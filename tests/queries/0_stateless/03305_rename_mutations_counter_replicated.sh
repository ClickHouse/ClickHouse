#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

set -e

function wait_for_mutation_cleanup()
{
    for _ in {0..50}; do
        res=`$CLICKHOUSE_CLIENT --query "SELECT value FROM system.metrics WHERE metric = 'ActiveMetadataMutations'"`
        if [[ $res == "0" ]]; then
            break
        fi
        sleep 0.5
    done
}

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_mutations_counters_rename;

    CREATE TABLE t_mutations_counters_rename (a UInt64, b UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mutations_counters_rename', '1') ORDER BY a;

    INSERT INTO t_mutations_counters_rename VALUES (1, 2) (2, 3);

    SET alter_sync = 0;
    SET mutations_sync = 0;

    SYSTEM STOP MERGES t_mutations_counters_rename;
    ALTER TABLE t_mutations_counters_rename RENAME COLUMN b TO c;

    SELECT metric, value FROM system.metrics WHERE metric = 'ActiveMetadataMutations';
    SYSTEM START MERGES t_mutations_counters_rename;
"

wait_for_mutation "t_mutations_counters_rename" "0000000000"
wait_for_mutation_cleanup

$CLICKHOUSE_CLIENT --query "
    SELECT metric, value FROM system.metrics WHERE metric = 'ActiveMetadataMutations';
    SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_mutations_counters_rename' AND NOT is_done;
    DROP TABLE t_mutations_counters_rename;
"
