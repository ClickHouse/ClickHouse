#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database, no-async-insert
# Tag no-parallel: Fails due to failpoint intersection
# no-replicated-database: Fails due to additional replicas or shards

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

set -e

# disable fault injection; part ids are non-deterministic in case of insert retries
$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0;

    CREATE TABLE rmt (id UInt64, num UInt64)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/rmt/', '1')
    ORDER BY id;

    INSERT INTO rmt VALUES (1, 1);
    INSERT INTO rmt VALUES (2, 2);
    INSERT INTO rmt VALUES (3, 3);

    SYSTEM SYNC REPLICA rmt;
"
# part in progress: all_0_0_0
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    ALTER TABLE rmt UPDATE num = num + 1 WHERE 1;
    SYSTEM WAIT FAILPOINT rmt_merge_selecting_task_pause_when_scheduled PAUSE;
    SYSTEM ENABLE FAILPOINT rmt_mutate_task_pause_in_prepare;
    SYSTEM NOTIFY FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT rmt_mutate_task_pause_in_prepare PAUSE;

    SELECT mutation_id, command, parts_to_do_names, parts_in_progress_names, is_done \
    FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'rmt' ORDER BY \
    mutation_id;

    SYSTEM NOTIFY FAILPOINT rmt_mutate_task_pause_in_prepare;
"
# wait for mutation of all_0_0_0 to finish
wait_for_mutation_in_progress "rmt" "0000000000" 0

# part in progress: all_1_1_0
$CLICKHOUSE_CLIENT --query "
    SYSTEM WAIT FAILPOINT rmt_merge_selecting_task_pause_when_scheduled PAUSE;
    SYSTEM NOTIFY FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT rmt_mutate_task_pause_in_prepare PAUSE;

    SELECT mutation_id, command, parts_to_do_names, parts_in_progress_names, is_done \
    FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'rmt' ORDER BY \
    mutation_id;

    SYSTEM NOTIFY FAILPOINT rmt_mutate_task_pause_in_prepare;
"

wait_for_mutation_in_progress "rmt" "0000000000" 0

# part in progress: all_2_2_0
$CLICKHOUSE_CLIENT --query "
    SYSTEM WAIT FAILPOINT rmt_merge_selecting_task_pause_when_scheduled PAUSE;
    SYSTEM NOTIFY FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT rmt_mutate_task_pause_in_prepare PAUSE;

    SELECT mutation_id, command, parts_to_do_names, parts_in_progress_names, is_done \
    FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'rmt' ORDER BY \
    mutation_id;

    SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_in_prepare;
    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE rmt SYNC;
"
