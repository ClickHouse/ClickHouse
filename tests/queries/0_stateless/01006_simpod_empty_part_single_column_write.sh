#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS table_with_empty_part"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE table_with_empty_part
(
    id UInt64,
    value UInt64
)
ENGINE = MergeTree()
ORDER BY id
PARTITION BY id
SETTINGS vertical_merge_algorithm_min_rows_to_activate=0, vertical_merge_algorithm_min_columns_to_activate=0, remove_empty_parts = 0
"


${CLICKHOUSE_CLIENT} --query="INSERT INTO table_with_empty_part VALUES (1, 1)"

${CLICKHOUSE_CLIENT} --query="INSERT INTO table_with_empty_part VALUES (2, 2)"

${CLICKHOUSE_CLIENT} --mutations_sync=2 --query="ALTER TABLE table_with_empty_part DELETE WHERE id % 2 == 0"

${CLICKHOUSE_CLIENT} --query="SELECT COUNT(DISTINCT value) FROM table_with_empty_part"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE table_with_empty_part MODIFY COLUMN value Nullable(UInt64)"

${CLICKHOUSE_CLIENT} --query="SELECT COUNT(distinct value) FROM table_with_empty_part"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE table_with_empty_part FINAL"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS table_with_empty_part"
