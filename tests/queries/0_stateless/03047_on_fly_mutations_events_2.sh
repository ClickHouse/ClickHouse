#!/usr/bin/env bash
# Tags: no-shared-merge-tree, no-parallel, no-replicated-database, no-parallel-replicas
# no-shared-merge-tree - this test relies that there are different
# parts on different replicas which is inapplicable for SharedMergeTree.
# no-replicated-database: fails due to additional replicas or shards.
# no-parallel-replicas: profile events may differ with parallel replicas.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function wait_for_mutation_partially()
{
    local table=$1
    local mutation_id=$2
    local parts_to_do=$3

    for i in {1..100}
    do
        sleep 0.1
        if [[ $(${CLICKHOUSE_CLIENT} --query="SELECT parts_to_do FROM system.mutations WHERE database='${CLICKHOUSE_DATABASE}' AND table='$table' AND mutation_id='$mutation_id'") -eq $parts_to_do ]]; then
            break
        fi

        if [[ $i -eq 100 ]]; then
            echo "Timed out while waiting for mutation to execute!"
        fi

    done
}

# After all manipulations with SYSTEM queries
# Replica 1 should have 1 mutated part, 1 not mutated.
# Replica 2 should have 2 not mutated parts.

${CLICKHOUSE_CLIENT} -n --query "
DROP TABLE IF EXISTS t_mutation_events_1 SYNC;
DROP TABLE IF EXISTS t_mutation_events_2 SYNC;

CREATE TABLE t_mutation_events_1 (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mutation_events', '1') ORDER BY id;

CREATE TABLE t_mutation_events_2 (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mutation_events', '2') ORDER BY id;

INSERT INTO t_mutation_events_1 VALUES (1, 10);
SYSTEM SYNC REPLICA t_mutation_events_2;

SYSTEM STOP FETCHES t_mutation_events_1;
SYSTEM STOP MERGES t_mutation_events_2;

INSERT INTO t_mutation_events_2 VALUES (2, 20);

ALTER TABLE t_mutation_events_1 UPDATE v = v * v WHERE 1;
"

wait_for_mutation_partially t_mutation_events_1 0000000000 1

${CLICKHOUSE_CLIENT} -n --query "
SYSTEM STOP MERGES t_mutation_events_1;
SYSTEM START FETCHES t_mutation_events_1;

SYSTEM SYNC REPLICA t_mutation_events_1 LIGHTWEIGHT;
SYSTEM SYNC REPLICA t_mutation_events_2 PULL;

SELECT * FROM t_mutation_events_1 ORDER BY id SETTINGS apply_mutations_on_fly = 0;
SELECT * FROM t_mutation_events_1 ORDER BY id SETTINGS apply_mutations_on_fly = 1;

SELECT * FROM t_mutation_events_2 ORDER BY id SETTINGS apply_mutations_on_fly = 0;
SELECT * FROM t_mutation_events_2 ORDER BY id SETTINGS apply_mutations_on_fly = 1;

SYSTEM FLUSH LOGS query_log;

SELECT
    query,
    ProfileEvents['ReadTasksWithAppliedMutationsOnFly'],
    ProfileEvents['MutationsAppliedOnFlyInAllReadTasks']
FROM system.query_log
WHERE current_database = currentDatabase() AND query ILIKE 'SELECT%FROM t_mutation_events_%' AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

DROP TABLE t_mutation_events_1;
"
