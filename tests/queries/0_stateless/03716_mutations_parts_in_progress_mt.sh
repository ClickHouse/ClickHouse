#!/usr/bin/env bash
# Tags: no-parallel
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

    INSERT INTO mt VALUES (1, 1) (2, 2) (3, 3);
"

# Test one mutation for partitions.
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT mt_mutate_task_pause_in_prepare;
    ALTER TABLE mt UPDATE num = num + 1 WHERE 1;
"

wait_for_mutation_in_progress "mt" "mutation_2.txt"

$CLICKHOUSE_CLIENT --query "
    SELECT mutation_id, command, parts_to_do_names, parts_in_progress_names, is_done \
    FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'mt' ORDER BY \
    mutation_id;
    SYSTEM DISABLE FAILPOINT mt_mutate_task_pause_in_prepare;
"

wait_for_mutation "mt" "mutation_2.txt"

# Test multiple mutations for partitions.
$CLICKHOUSE_CLIENT --query "
    SYSTEM STOP MERGES mt;
    ALTER TABLE mt UPDATE num = num + 2 WHERE 1;
    ALTER TABLE mt UPDATE num = num + 3 WHERE 1;

    SYSTEM ENABLE FAILPOINT mt_mutate_task_pause_in_prepare;
    SYSTEM START MERGES mt;
"

wait_for_mutation_in_progress "mt" "mutation_4.txt"

$CLICKHOUSE_CLIENT --query "
    SELECT mutation_id, command, parts_to_do_names, parts_in_progress_names, is_done \
    FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'mt' ORDER BY \
    mutation_id;
    SYSTEM DISABLE FAILPOINT mt_mutate_task_pause_in_prepare;
"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE mt SYNC;
"
