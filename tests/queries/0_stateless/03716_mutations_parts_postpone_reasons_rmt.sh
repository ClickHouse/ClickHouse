#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database
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
    ORDER BY id
    SETTINGS
        merge_selecting_sleep_ms = 100,
        max_merge_selecting_sleep_ms = 200;

    INSERT INTO rmt VALUES (1, 1), (2, 2), (3, 3);
"

#test1 'all_parts'->no thread in pool for one mutation
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT rmt_merge_selecting_task_pause_when_scheduled PAUSE;
    ALTER TABLE rmt UPDATE num = num + 1 WHERE 1;
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_no_free_threads;
    SYSTEM NOTIFY FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT rmt_merge_selecting_task_pause_when_scheduled PAUSE;
"

$CLICKHOUSE_CLIENT --query "
    SELECT mutation_id, command, parts_to_do_names, parts_in_progress_names, parts_postpone_reasons, \
    is_done FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'rmt' ORDER BY \
    mutation_id;
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_no_free_threads;
"

wait_for_mutation "rmt" "0000000000"

#test2 'all_parts'->no thread in pool for multiple mutations
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT rmt_merge_selecting_task_pause_when_scheduled PAUSE;
    ALTER TABLE rmt UPDATE num = num + 2 WHERE 1;
    ALTER TABLE rmt UPDATE num = num + 3 WHERE 1;
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_no_free_threads;
    SYSTEM NOTIFY FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT rmt_merge_selecting_task_pause_when_scheduled PAUSE;
"

$CLICKHOUSE_CLIENT --query "
    SELECT mutation_id, command, parts_to_do_names, parts_in_progress_names, parts_postpone_reasons, \
    is_done FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'rmt' ORDER BY \
    mutation_id;
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_no_free_threads;
    DROP TABLE rmt SYNC;
"

$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0;

    CREATE TABLE rmt (id UInt64, num UInt64)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/rmt/', '1')
    ORDER BY id
    SETTINGS
        merge_selecting_sleep_ms = 100,
        max_merge_selecting_sleep_ms = 200;

    INSERT INTO rmt VALUES (1, 1), (2, 2), (3, 3);
"

#test3 part->postpone reasons for one mutation
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT rmt_merge_selecting_task_pause_when_scheduled PAUSE;
    ALTER TABLE rmt UPDATE num = num + 1 WHERE 1;
"

# Wait for the mutation to be pulled from ZooKeeper into the in-memory queue,
# otherwise the merge selecting task may not see it in countMutations().
for _ in {1..300}; do
    if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'rmt' AND NOT is_done") -gt 0 ]]; then
        break
    fi
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_max_part_size;
    SYSTEM NOTIFY FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT rmt_merge_selecting_task_pause_when_scheduled PAUSE;
"

$CLICKHOUSE_CLIENT --query "
    SELECT mutation_id, command, parts_to_do_names, parts_in_progress_names, parts_postpone_reasons, \
    is_done FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'rmt' ORDER BY \
    mutation_id;
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_max_part_size;
"

wait_for_mutation "rmt" "0000000000"

#test4 part->postpone reasons for multiple mutations
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT rmt_merge_selecting_task_pause_when_scheduled PAUSE;
    ALTER TABLE rmt UPDATE num = num + 2 WHERE 1;
    ALTER TABLE rmt UPDATE num = num + 3 WHERE 1;
"

# Wait for both mutations to be pulled from ZooKeeper into the in-memory queue.
for _ in {1..300}; do
    if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'rmt' AND NOT is_done") -ge 2 ]]; then
        break
    fi
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_max_part_size;
    SYSTEM NOTIFY FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT rmt_merge_selecting_task_pause_when_scheduled PAUSE;
"

$CLICKHOUSE_CLIENT --query "
    SELECT mutation_id, command, parts_to_do_names, parts_in_progress_names, parts_postpone_reasons, \
    is_done FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'rmt' ORDER BY \
    mutation_id;
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_max_part_size;
    DROP TABLE rmt SYNC;
"
