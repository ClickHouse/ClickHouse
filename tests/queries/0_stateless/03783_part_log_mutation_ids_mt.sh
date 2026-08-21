#!/usr/bin/env bash
# Tags: no-parallel-replicas

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
    ALTER TABLE mt UPDATE num = num + 1 WHERE 1;
"

wait_for_mutation "mt" "mutation_2.txt"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS part_log;"

$CLICKHOUSE_CLIENT --query "
    SELECT part_name, event_type, merged_from, mutation_ids \
    FROM system.part_log WHERE database = '$CLICKHOUSE_DATABASE' and table = 'mt' \
    AND event_type IN ('MutatePart', 'MutatePartStart') \
    ORDER BY event_time_microseconds;
"

# Test multiple mutations for partitions.
$CLICKHOUSE_CLIENT --query "
    SYSTEM STOP MERGES mt;
    ALTER TABLE mt UPDATE num = num + 2 WHERE 1;
    ALTER TABLE mt UPDATE num = num + 3 WHERE 1;
    ALTER TABLE mt UPDATE num = num + 4 WHERE 1;
    SYSTEM START MERGES mt;
"

wait_for_mutation "mt" "mutation_5.txt"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS part_log;"

$CLICKHOUSE_CLIENT --query "
    SELECT part_name, event_type, merged_from, mutation_ids \
    FROM system.part_log WHERE database = '$CLICKHOUSE_DATABASE' and table = 'mt' \
    AND event_type IN ('MutatePart', 'MutatePartStart') \
    ORDER BY event_time_microseconds;
"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE mt SYNC;
"
