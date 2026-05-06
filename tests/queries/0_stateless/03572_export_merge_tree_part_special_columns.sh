#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires s3 storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

mt_alias="mt_alias_${RANDOM}"
mt_materialized="mt_materialized_${RANDOM}"
s3_alias_export="s3_alias_export_${RANDOM}"
s3_materialized_export="s3_materialized_export_${RANDOM}"
mt_mixed="mt_mixed_${RANDOM}"
s3_mixed_export="s3_mixed_export_${RANDOM}"
mt_complex_expr="mt_complex_expr_${RANDOM}"
s3_complex_expr_export="s3_complex_expr_export_${RANDOM}"
mt_ephemeral="mt_ephemeral_${RANDOM}"
s3_ephemeral_export="s3_ephemeral_export_${RANDOM}"
s3_mixed_export_table_function="s3_mixed_export_table_function_${RANDOM}"

query() {
    $CLICKHOUSE_CLIENT --query "$1"
}

query "DROP TABLE IF EXISTS $mt_alias, $mt_materialized, $s3_alias_export, $s3_materialized_export, $mt_mixed, $s3_mixed_export, $mt_complex_expr, $s3_complex_expr_export, $mt_ephemeral, $s3_ephemeral_export"

# Create all tables
echo "---- Test ALIAS columns export"
query "CREATE TABLE $mt_alias (a UInt32, arr Array(UInt64), arr_1 UInt64 ALIAS arr[1]) ENGINE = MergeTree() PARTITION BY a ORDER BY (a, arr[1]) SETTINGS index_granularity = 1"
query "CREATE TABLE $s3_alias_export (a UInt32, arr Array(UInt64), arr_1 UInt64) ENGINE = S3(s3_conn, filename='$s3_alias_export', format=Parquet, partition_strategy='hive') PARTITION BY a"

echo "---- Test MATERIALIZED columns export"
query "CREATE TABLE $mt_materialized (a UInt32, arr Array(UInt64), arr_1 UInt64 MATERIALIZED arr[1]) ENGINE = MergeTree() PARTITION BY a ORDER BY (a, arr_1) SETTINGS index_granularity = 1"
query "CREATE TABLE $s3_materialized_export (a UInt32, arr Array(UInt64), arr_1 UInt64) ENGINE = S3(s3_conn, filename='$s3_materialized_export', format=Parquet, partition_strategy='hive') PARTITION BY a"

echo "---- Test EPHEMERAL column (not stored, ignored during export)"
query "CREATE TABLE $mt_ephemeral (
    id UInt32,
    name_input String EPHEMERAL,
    name_upper String DEFAULT upper(name_input)
) ENGINE = MergeTree() PARTITION BY id ORDER BY id SETTINGS index_granularity = 1"

query "CREATE TABLE $s3_ephemeral_export (
    id UInt32,
    name_upper String
) ENGINE = S3(s3_conn, filename='$s3_ephemeral_export', format=Parquet, partition_strategy='hive') PARTITION BY id"

echo "---- Test Mixed ALIAS, MATERIALIZED, and EPHEMERAL in same table"
query "CREATE TABLE $mt_mixed (
    id UInt32,
    value UInt32,
    tag_input String EPHEMERAL,
    doubled UInt64 ALIAS value * 2,
    tripled UInt64 MATERIALIZED value * 3,
    tag String DEFAULT upper(tag_input)
) ENGINE = MergeTree() PARTITION BY id ORDER BY id SETTINGS index_granularity = 1"

query "CREATE TABLE $s3_mixed_export (
    id UInt32,
    value UInt32,
    doubled UInt64,
    tripled UInt64,
    tag String
) ENGINE = S3(s3_conn, filename='$s3_mixed_export', format=Parquet, partition_strategy='hive') PARTITION BY id"

echo "---- Test Complex Expressions in computed columns"
query "CREATE TABLE $mt_complex_expr (
    id UInt32,
    name String,
    upper_name String ALIAS upper(name),
    concat_result String MATERIALIZED concat(name, '-', toString(id))
) ENGINE = MergeTree() PARTITION BY id ORDER BY id SETTINGS index_granularity = 1"

query "CREATE TABLE $s3_complex_expr_export (
    id UInt32,
    name String,
    upper_name String,
    concat_result String
) ENGINE = S3(s3_conn, filename='$s3_complex_expr_export', format=Parquet, partition_strategy='hive') PARTITION BY id"

# Insert all data
query "INSERT INTO $mt_alias VALUES (1, [1, 2, 3]), (1, [10, 20, 30])"
query "INSERT INTO $mt_materialized VALUES (1, [1, 2, 3]), (1, [10, 20, 30])"
query "INSERT INTO $mt_ephemeral (id, name_input) VALUES (1, 'alice'), (1, 'bob')"
query "INSERT INTO $mt_mixed (id, value, tag_input) VALUES (1, 5, 'test'), (1, 10, 'prod')"
query "INSERT INTO $mt_mixed (id, value, tag_input) VALUES (2, 15, 'dev')"
query "INSERT INTO $mt_complex_expr (id, name) VALUES (1, 'alice'), (1, 'bob')"

# Get all part names
alias_part=$(query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$mt_alias' AND partition_id = '1' AND active = 1 ORDER BY name LIMIT 1" | tr -d '\n')
materialized_part=$(query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$mt_materialized' AND partition_id = '1' AND active = 1 ORDER BY name LIMIT 1" | tr -d '\n')
ephemeral_part=$(query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$mt_ephemeral' AND partition_id = '1' AND active = 1 ORDER BY name LIMIT 1" | tr -d '\n')
mixed_part=$(query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$mt_mixed' AND partition_id = '1' AND active = 1 ORDER BY name LIMIT 1" | tr -d '\n')
mixed_part_2=$(query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$mt_mixed' AND partition_id = '2' AND active = 1 ORDER BY name LIMIT 1" | tr -d '\n')
complex_expr_part=$(query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$mt_complex_expr' AND partition_id = '1' AND active = 1 ORDER BY name LIMIT 1" | tr -d '\n')

# ============================================================================
# ALL EXPORTS HAPPEN HERE
# ============================================================================

query "ALTER TABLE $mt_alias EXPORT PART '$alias_part' TO TABLE $s3_alias_export SETTINGS allow_experimental_export_merge_tree_part = 1"

query "ALTER TABLE $mt_materialized EXPORT PART '$materialized_part' TO TABLE $s3_materialized_export SETTINGS allow_experimental_export_merge_tree_part = 1"

query "ALTER TABLE $mt_ephemeral EXPORT PART '$ephemeral_part' TO TABLE $s3_ephemeral_export SETTINGS allow_experimental_export_merge_tree_part = 1"

query "ALTER TABLE $mt_mixed EXPORT PART '$mixed_part' TO TABLE $s3_mixed_export SETTINGS allow_experimental_export_merge_tree_part = 1"

echo "---- Test Export to Table Function with mixed columns"
query "ALTER TABLE $mt_mixed EXPORT PART '$mixed_part_2' TO TABLE FUNCTION s3(s3_conn, filename='$s3_mixed_export_table_function', format=Parquet, partition_strategy='hive') PARTITION BY id SETTINGS allow_experimental_export_merge_tree_part = 1"

query "ALTER TABLE $mt_complex_expr EXPORT PART '$complex_expr_part' TO TABLE $s3_complex_expr_export SETTINGS allow_experimental_export_merge_tree_part = 1"

# ONE BIG SLEEP after all exports
sleep 20

# ============================================================================
# ALL SELECTS/VERIFICATIONS HAPPEN HERE
# ============================================================================

echo "---- Verify ALIAS column data in source table (arr_1 computed from arr[1])"
query "SELECT a, arr, arr_1 FROM $mt_alias ORDER BY arr"

echo "---- Verify ALIAS column data exported to S3 (should match source)"
query "SELECT a, arr, arr_1 FROM $s3_alias_export ORDER BY arr"

echo "---- Verify MATERIALIZED column data in source table (arr_1 computed from arr[1])"
query "SELECT a, arr, arr_1 FROM $mt_materialized ORDER BY arr"

echo "---- Verify MATERIALIZED column data exported to S3 (should match source)"
query "SELECT a, arr, arr_1 FROM $s3_materialized_export ORDER BY arr"

echo "---- Verify data in source"
query "SELECT id, name_upper FROM $mt_ephemeral ORDER BY name_upper"

echo "---- Verify exported data"
query "SELECT id, name_upper FROM $s3_ephemeral_export ORDER BY name_upper"

echo "---- Verify mixed columns in source table"
query "SELECT id, value, doubled, tripled, tag FROM $mt_mixed ORDER BY value"

echo "---- Verify mixed columns exported to S3"
query "SELECT id, value, doubled, tripled, tag FROM $s3_mixed_export ORDER BY value"

echo "---- Verify mixed columns exported to S3"
query "SELECT * FROM s3(s3_conn, filename='$s3_mixed_export_table_function/**.parquet', format=Parquet) ORDER BY value"

echo "---- Verify complex expressions in source table"
query "SELECT id, name, upper_name, concat_result FROM $mt_complex_expr ORDER BY name"

echo "---- Verify complex expressions exported to S3 (should match source)"
query "SELECT id, name, upper_name, concat_result FROM $s3_complex_expr_export ORDER BY name"

query "DROP TABLE IF EXISTS $mt_alias, $mt_materialized, $s3_alias_export, $s3_materialized_export, $mt_ephemeral, $s3_ephemeral_export, $mt_mixed, $s3_mixed_export, $mt_complex_expr, $s3_complex_expr_export"
