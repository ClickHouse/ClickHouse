#!/usr/bin/env bash
# Contract of `allow_tuple_element_aggregation`: immutable after creation;
# when enabled, plain Tuple sorting key is rejected on CREATE and ATTACH;
# silently ignored by engines that don't support it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# Immutability.
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_alter_table (\`n\` Tuple(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple();"
$CLICKHOUSE_CLIENT -q "ALTER TABLE test_alter_table MODIFY SETTING allow_tuple_element_aggregation = 1;" 2>&1 | grep -m 1 -o -F 'READONLY_SETTING'
$CLICKHOUSE_CLIENT -q "DROP TABLE test_alter_table;"

# CREATE path.
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_tuple_order (\`n\` Tuple(a UInt32, b UInt32)) ENGINE = SummingMergeTree ORDER BY n SETTINGS allow_tuple_element_aggregation = 1;" 2>&1 | grep -m 1 -o -F 'NOT_IMPLEMENTED'

# ATTACH path: generate a random UUID to avoid collisions in Atomic databases.
# -m1 because the error message contains the error code name multiple times.
UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT -q "ATTACH TABLE test_attach_reject UUID '${UUID}' (\`n\` Tuple(a UInt32, b UInt32)) ENGINE = SummingMergeTree ORDER BY n SETTINGS allow_tuple_element_aggregation = 1;" 2>&1 | grep -m 1 -o -F 'NOT_IMPLEMENTED'

# Unsupported engines accept the setting silently.
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_replacing_engine (\`n\` Tuple(a UInt32, b UInt32)) ENGINE = ReplacingMergeTree ORDER BY tuple() SETTINGS allow_tuple_element_aggregation = 1;"
$CLICKHOUSE_CLIENT -q "DROP TABLE test_replacing_engine;"

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_collapsing_engine (\`n\` Tuple(a UInt32, b UInt32), \`sign\` Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY tuple() SETTINGS allow_tuple_element_aggregation = 1;"
$CLICKHOUSE_CLIENT -q "DROP TABLE test_collapsing_engine;"

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_normal_engine (\`n\` Tuple(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_tuple_element_aggregation = 1;"
$CLICKHOUSE_CLIENT -q "DROP TABLE test_normal_engine;"
