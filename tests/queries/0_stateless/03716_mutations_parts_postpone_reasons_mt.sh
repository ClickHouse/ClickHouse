#!/usr/bin/env bash
# Tags: no-parallel, no-async-insert
# Tag no-parallel: Fails due to failpoint intersection

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

set -e

# Wait until at least one non-done mutation for the given table has non-empty
# parts_postpone_reasons. The failpoints that block mutation selection keep the
# state stable once set, so polling is safe.
function wait_for_postpone_reasons()
{
    local table=$1
    for _ in $(seq 1 300); do
        result=$($CLICKHOUSE_CLIENT --query "
            SELECT count()
            FROM system.mutations
            WHERE database = '$CLICKHOUSE_DATABASE' AND table = '$table'
              AND NOT is_done AND notEmpty(parts_postpone_reasons)
        ")
        if [ "$result" -gt 0 ]; then
            return 0
        fi
        sleep 0.1
    done
    echo "Timed out waiting for parts_postpone_reasons" >&2
    return 1
}

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE mt (id UInt64, num UInt64)
    ENGINE = MergeTree()
    ORDER BY id;

    INSERT INTO mt VALUES (1, 1), (2, 2), (3, 3);
"

#test1 'all_parts'->no thread in pool for one mutation
# Enable the failpoint before creating the mutation to guarantee every
# selectPartsToMutate call sees it. No pause failpoint needed: since the
# failpoint blocks mutation selection, the state is stable once set.
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    ALTER TABLE mt UPDATE num = num + 1 WHERE 1;
"

wait_for_postpone_reasons "mt"

$CLICKHOUSE_CLIENT --query "
    SELECT mutation_id, command, parts_to_do_names, parts_in_progress_names, parts_postpone_reasons, \
    is_done FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'mt' ORDER BY \
    mutation_id;
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
"

wait_for_mutation "mt" "mutation_2.txt"

#test2 'all_parts'->no thread in pool for multiple mutations
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    ALTER TABLE mt UPDATE num = num + 2 WHERE 1;
    ALTER TABLE mt UPDATE num = num + 3 WHERE 1;
"

wait_for_postpone_reasons "mt"

$CLICKHOUSE_CLIENT --query "
    SELECT mutation_id, command, parts_to_do_names, parts_in_progress_names, parts_postpone_reasons, \
    is_done FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'mt' ORDER BY \
    mutation_id;
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    DROP TABLE mt SYNC;
"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE mt (id UInt64, num UInt64)
    ENGINE = MergeTree()
    ORDER BY id;

    INSERT INTO mt VALUES (1, 1), (2, 2), (3, 3);
"

#test3 part->postpone reasons in pool for one mutation
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;
    ALTER TABLE mt UPDATE num = num + 1 WHERE 1;
"

wait_for_postpone_reasons "mt"

$CLICKHOUSE_CLIENT --query "
    SELECT mutation_id, command, parts_to_do_names, parts_in_progress_names, parts_postpone_reasons, \
    is_done FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'mt' ORDER BY \
    mutation_id;
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;
"

wait_for_mutation "mt" "mutation_2.txt"

#test4 part->postpone reasons for multiple mutations
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;
    ALTER TABLE mt UPDATE num = num + 2 WHERE 1;
    ALTER TABLE mt UPDATE num = num + 3 WHERE 1;
"

wait_for_postpone_reasons "mt"

$CLICKHOUSE_CLIENT --query "
    SELECT mutation_id, command, parts_to_do_names, parts_in_progress_names, parts_postpone_reasons, \
    is_done FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'mt' ORDER BY \
    mutation_id;
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;
    DROP TABLE mt SYNC;
"
