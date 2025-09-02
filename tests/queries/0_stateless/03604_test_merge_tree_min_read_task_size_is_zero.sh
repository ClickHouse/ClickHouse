#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_NAME="table_${RANDOM}${RANDOM}"

$CLICKHOUSE_CLIENT -q "SET allow_experimental_lightweight_update = 1;"

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${TABLE_NAME} (c0 Int) ENGINE = MergeTree() ORDER BY tuple()
 SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;"
$CLICKHOUSE_CLIENT -q "INSERT INTO TABLE ${TABLE_NAME} (c0) VALUES (1);"
$CLICKHOUSE_CLIENT -q "DELETE FROM ${TABLE_NAME} WHERE c0 = 2;"
$CLICKHOUSE_CLIENT -q "UPDATE ${TABLE_NAME} SET c0 = 3 WHERE TRUE;"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM ${TABLE_NAME} SETTINGS apply_mutations_on_fly = 1,
 merge_tree_min_read_task_size = 0 -- { serverError INVALID_SETTING_VALUE }"

$CLICKHOUSE_CLIENT -q "DROP TABLE ${TABLE_NAME};"