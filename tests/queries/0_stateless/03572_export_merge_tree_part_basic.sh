#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires s3 storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

mt_table="mt_table_${RANDOM}"
mt_table_partition_expression_with_function="mt_table_partition_expression_with_function_${RANDOM}"
s3_table="s3_table_${RANDOM}"
s3_table_wildcard="s3_table_wildcard_${RANDOM}"
s3_table_wildcard_partition_expression_with_function="s3_table_wildcard_partition_expression_with_function_${RANDOM}"
mt_table_roundtrip="mt_table_roundtrip_${RANDOM}"

query() {
    $CLICKHOUSE_CLIENT --query "$1"
}

query "DROP TABLE IF EXISTS $mt_table, $s3_table, $mt_table_roundtrip, $s3_table_wildcard, $s3_table_wildcard_partition_expression_with_function, $mt_table_partition_expression_with_function"

# Create all tables
query "CREATE TABLE $mt_table (id UInt64, year UInt16) ENGINE = MergeTree() PARTITION BY year ORDER BY tuple()"
query "CREATE TABLE $s3_table (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='$s3_table', format=Parquet, partition_strategy='hive') PARTITION BY year"
query "CREATE TABLE $s3_table_wildcard (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='$s3_table_wildcard/{_partition_id}/{_file}.parquet', format=Parquet, partition_strategy='wildcard') PARTITION BY year"
query "CREATE TABLE $mt_table_partition_expression_with_function (id UInt64, year UInt16) ENGINE = MergeTree() PARTITION BY toString(year) ORDER BY tuple()"
query "CREATE TABLE $s3_table_wildcard_partition_expression_with_function (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='$s3_table_wildcard_partition_expression_with_function/{_partition_id}/{_file}.parquet', format=Parquet, partition_strategy='wildcard') PARTITION BY toString(year)"

# Insert all data
query "INSERT INTO $mt_table VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021), (5, 2022), (6, 2022), (7, 2023), (8, 2023), (9, 2024), (10, 2024), (11, 2025), (12, 2025)"
query "INSERT INTO $mt_table_partition_expression_with_function VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021)"

# ============================================================================
# ALL EXPORTS HAPPEN HERE
# ============================================================================

echo "---- Export 1: Export 2020_1_1_0 and 2021_2_2_0"
query "ALTER TABLE $mt_table EXPORT PART '2020_1_1_0' TO TABLE $s3_table SETTINGS allow_experimental_export_merge_tree_part = 1"
query "ALTER TABLE $mt_table EXPORT PART '2021_2_2_0' TO TABLE $s3_table SETTINGS allow_experimental_export_merge_tree_part = 1"

echo "---- Export 2: Export 2022_3_3_0 and 2023_4_4_0 to wildcard table"
query "ALTER TABLE $mt_table EXPORT PART '2022_3_3_0' TO TABLE $s3_table_wildcard SETTINGS allow_experimental_export_merge_tree_part = 1"
query "ALTER TABLE $mt_table EXPORT PART '2023_4_4_0' TO TABLE $s3_table_wildcard SETTINGS allow_experimental_export_merge_tree_part = 1"

echo "---- Export 3: Export 2020_1_1_0 and 2021_2_2_0 to wildcard table with partition expression with function"
query "ALTER TABLE $mt_table_partition_expression_with_function EXPORT PART 'cb217c742dc7d143b61583011996a160_1_1_0' TO TABLE $s3_table_wildcard_partition_expression_with_function SETTINGS allow_experimental_export_merge_tree_part = 1"
query "ALTER TABLE $mt_table_partition_expression_with_function EXPORT PART '3be6d49ecf9749a383964bc6fab22d10_2_2_0' TO TABLE $s3_table_wildcard_partition_expression_with_function SETTINGS allow_experimental_export_merge_tree_part = 1"

# below exports are using parts that were exported in export 1 and export 2, so we need to wait for them to complete
sleep 5

echo "---- Export 4: Export the same part again, it should be idempotent"
query "ALTER TABLE $mt_table EXPORT PART '2020_1_1_0' TO TABLE $s3_table SETTINGS allow_experimental_export_merge_tree_part = 1"

echo "---- Export 5: Export the same part again to wildcard, it should be idempotent"
query "ALTER TABLE $mt_table EXPORT PART '2022_3_3_0' TO TABLE $s3_table_wildcard SETTINGS allow_experimental_export_merge_tree_part = 1"

# ONE BIG SLEEP after all exports
sleep 15

# ============================================================================
# ALL SELECTS/VERIFICATIONS HAPPEN HERE
# ============================================================================

echo "---- Verify Export 1: Both data parts should appear"
query "SELECT * FROM $s3_table ORDER BY id"

echo "---- Verify Export 4: Export the same part again, it should be idempotent"
query "SELECT * FROM $s3_table ORDER BY id"

query "CREATE TABLE $mt_table_roundtrip ENGINE = MergeTree() PARTITION BY year ORDER BY tuple() AS SELECT * FROM $s3_table"

echo "---- Verify: Data in roundtrip MergeTree table (should match s3_table)"
query "SELECT * FROM $mt_table_roundtrip ORDER BY id"

echo "---- Verify Export 2: Both data parts should appear (2022_3_3_0 and 2023_4_4_0)"
query "SELECT * FROM s3(s3_conn, filename='$s3_table_wildcard/**.parquet') ORDER BY id"

echo "---- Verify Export 5: Export the same part again, it should be idempotent"
query "SELECT * FROM s3(s3_conn, filename='$s3_table_wildcard/**.parquet') ORDER BY id"

echo "---- Verify Export 3: Both data parts should appear"
query "SELECT * FROM s3(s3_conn, filename='$s3_table_wildcard_partition_expression_with_function/**.parquet') ORDER BY id"

query "DROP TABLE IF EXISTS $mt_table, $s3_table, $mt_table_roundtrip, $s3_table_wildcard, $s3_table_wildcard_partition_expression_with_function, $mt_table_partition_expression_with_function"
