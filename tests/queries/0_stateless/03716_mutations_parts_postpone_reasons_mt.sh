#!/usr/bin/env bash
# Tags: no-parallel, no-async-insert
# Tag no-parallel: Fails due to failpoint intersection

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

set -e

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE mt (id UInt64, num UInt64)
    ENGINE = MergeTree()
    ORDER BY id;

    INSERT INTO mt VALUES (1, 1);
    INSERT INTO mt VALUES (2, 2);
    INSERT INTO mt VALUES (3, 3);
"

#test1 'all_parts'->no thread in pool for one mutation
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT mt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT mt_merge_selecting_task_pause_when_scheduled PAUSE;
    ALTER TABLE mt UPDATE num = num + 1 WHERE 1;
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    SYSTEM NOTIFY FAILPOINT mt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT mt_merge_selecting_task_pause_when_scheduled PAUSE;
"

$CLICKHOUSE_CLIENT --query "
    SELECT mutation_id, command, parts_to_do_names, parts_in_progress_names, parts_postpone_reasons, \
    is_done FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'mt' ORDER BY \
    mutation_id;
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT mt_merge_selecting_task_pause_when_scheduled;
    SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
"

wait_for_mutation "mt" "mutation_4.txt"

#test2 'all_parts'->no thread in pool for multiple mutations
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT mt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT mt_merge_selecting_task_pause_when_scheduled PAUSE;
    ALTER TABLE mt UPDATE num = num + 2 WHERE 1;
    ALTER TABLE mt UPDATE num = num + 3 WHERE 1;
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    SYSTEM NOTIFY FAILPOINT mt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT mt_merge_selecting_task_pause_when_scheduled PAUSE;
"

$CLICKHOUSE_CLIENT --query "
    SELECT mutation_id, command, parts_to_do_names, parts_in_progress_names, parts_postpone_reasons, \
    is_done FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'mt' ORDER BY \
    mutation_id;
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT mt_merge_selecting_task_pause_when_scheduled;
    SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    DROP TABLE mt SYNC;
"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE mt (id UInt64, num UInt64)
    ENGINE = MergeTree()
    ORDER BY id;

    INSERT INTO mt VALUES (1, 1);
    INSERT INTO mt VALUES (2, 2);
    INSERT INTO mt VALUES (3, 3);
"

#test3 part->postpone reasons in pool for one mutation
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT mt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT mt_merge_selecting_task_pause_when_scheduled PAUSE;
    ALTER TABLE mt UPDATE num = num + 1 WHERE 1;
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;
    SYSTEM NOTIFY FAILPOINT mt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT mt_merge_selecting_task_pause_when_scheduled PAUSE;
"

$CLICKHOUSE_CLIENT --query "
    SELECT mutation_id, command, parts_to_do_names, parts_in_progress_names, parts_postpone_reasons, \
    is_done FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'mt' ORDER BY \
    mutation_id;
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT mt_merge_selecting_task_pause_when_scheduled;
    SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;
"

wait_for_mutation "mt" "mutation_4.txt"

#test4 part->postpone reasons for multiple mutations
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT mt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT mt_merge_selecting_task_pause_when_scheduled PAUSE;
    ALTER TABLE mt UPDATE num = num + 2 WHERE 1;
    ALTER TABLE mt UPDATE num = num + 3 WHERE 1;
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;
    SYSTEM NOTIFY FAILPOINT mt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT mt_merge_selecting_task_pause_when_scheduled PAUSE;
"

$CLICKHOUSE_CLIENT --query "
    SELECT mutation_id, command, parts_to_do_names, parts_in_progress_names, parts_postpone_reasons, \
    is_done FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'mt' ORDER BY \
    mutation_id;
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT mt_merge_selecting_task_pause_when_scheduled;
    SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;
    DROP TABLE mt SYNC;
"
