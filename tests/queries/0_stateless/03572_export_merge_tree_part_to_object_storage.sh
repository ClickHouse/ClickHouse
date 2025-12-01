#!/usr/bin/env bash

# Tags: no-fasttest

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

query "CREATE TABLE $mt_table (id UInt64, year UInt16) ENGINE = MergeTree() PARTITION BY year ORDER BY tuple()"
query "CREATE TABLE $s3_table (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='$s3_table', format=Parquet, partition_strategy='hive') PARTITION BY year"

query "INSERT INTO $mt_table VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021)"
echo "---- Export 2020_1_1_0 and 2021_2_2_0"
query "ALTER TABLE $mt_table EXPORT PART '2020_1_1_0' TO TABLE $s3_table SETTINGS allow_experimental_export_merge_tree_part = 1"
query "ALTER TABLE $mt_table EXPORT PART '2021_2_2_0' TO TABLE $s3_table SETTINGS allow_experimental_export_merge_tree_part = 1"

echo "---- Both data parts should appear"
query "SELECT * FROM $s3_table ORDER BY id"

echo "---- Export the same part again, it should be idempotent"
query "ALTER TABLE $mt_table EXPORT PART '2020_1_1_0' TO TABLE $s3_table SETTINGS allow_experimental_export_merge_tree_part = 1"

query "SELECT * FROM $s3_table ORDER BY id"

query "CREATE TABLE $mt_table_roundtrip ENGINE = MergeTree() PARTITION BY year ORDER BY tuple() AS SELECT * FROM $s3_table"

echo "---- Data in roundtrip MergeTree table (should match s3_table)"
query "SELECT * FROM $s3_table ORDER BY id"

query "CREATE TABLE $s3_table_wildcard (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='$s3_table_wildcard/{_partition_id}/{_file}.parquet', format=Parquet, partition_strategy='wildcard') PARTITION BY year"

echo "---- Export 2020_1_1_0 and 2021_2_2_0 to wildcard table"
query "ALTER TABLE $mt_table EXPORT PART '2020_1_1_0' TO TABLE $s3_table_wildcard SETTINGS allow_experimental_export_merge_tree_part = 1"
query "ALTER TABLE $mt_table EXPORT PART '2021_2_2_0' TO TABLE $s3_table_wildcard SETTINGS allow_experimental_export_merge_tree_part = 1"

echo "---- Both data parts should appear"
query "SELECT * FROM s3(s3_conn, filename='$s3_table_wildcard/**.parquet') ORDER BY id"

echo "---- Export the same part again, it should be idempotent"
query "ALTER TABLE $mt_table EXPORT PART '2020_1_1_0' TO TABLE $s3_table_wildcard SETTINGS allow_experimental_export_merge_tree_part = 1"

query "SELECT * FROM s3(s3_conn, filename='$s3_table_wildcard/**.parquet') ORDER BY id"

query "CREATE TABLE $mt_table_partition_expression_with_function (id UInt64, year UInt16) ENGINE = MergeTree() PARTITION BY toString(year) ORDER BY tuple()"
query "CREATE TABLE $s3_table_wildcard_partition_expression_with_function (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='$s3_table_wildcard_partition_expression_with_function/{_partition_id}/{_file}.parquet', format=Parquet, partition_strategy='wildcard') PARTITION BY toString(year)"

# insert
query "INSERT INTO $mt_table_partition_expression_with_function VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021)"

echo "---- Export 2020_1_1_0 and 2021_2_2_0 to wildcard table with partition expression with function"
query "ALTER TABLE $mt_table_partition_expression_with_function EXPORT PART 'cb217c742dc7d143b61583011996a160_1_1_0' TO TABLE $s3_table_wildcard_partition_expression_with_function SETTINGS allow_experimental_export_merge_tree_part = 1"
query "ALTER TABLE $mt_table_partition_expression_with_function EXPORT PART '3be6d49ecf9749a383964bc6fab22d10_2_2_0' TO TABLE $s3_table_wildcard_partition_expression_with_function SETTINGS allow_experimental_export_merge_tree_part = 1"

echo "---- Both data parts should appear"
query "SELECT * FROM s3(s3_conn, filename='$s3_table_wildcard_partition_expression_with_function/**.parquet') ORDER BY id"

query "DROP TABLE IF EXISTS $mt_table, $s3_table, $mt_table_roundtrip, $s3_table_wildcard, $s3_table_wildcard_partition_expression_with_function, $mt_table_partition_expression_with_function"
