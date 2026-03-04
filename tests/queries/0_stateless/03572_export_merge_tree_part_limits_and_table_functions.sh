#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires s3 storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

big_table="big_table_${RANDOM}"
big_destination_max_bytes="big_destination_max_bytes_${RANDOM}"
big_destination_max_rows="big_destination_max_rows_${RANDOM}"
tf_schema_inherit="tf_schema_inherit_${RANDOM}"
tf_schema_explicit="tf_schema_explicit_${RANDOM}"
mt_table_tf="mt_table_tf_${RANDOM}"

query() {
    $CLICKHOUSE_CLIENT --query "$1"
}

query "DROP TABLE IF EXISTS $big_table, $big_destination_max_bytes, $big_destination_max_rows, $mt_table_tf"

echo "---- Test max_bytes and max_rows per file"

# Create all tables
query "CREATE TABLE $big_table (id UInt64, data String, year UInt16) Engine=MergeTree() order by id partition by year"
query "CREATE TABLE $big_destination_max_bytes(id UInt64, data String, year UInt16) engine=S3(s3_conn, filename='$big_destination_max_bytes', partition_strategy='hive', format=Parquet) partition by year"
query "CREATE TABLE $big_destination_max_rows(id UInt64, data String, year UInt16) engine=S3(s3_conn, filename='$big_destination_max_rows', partition_strategy='hive', format=Parquet) partition by year"
query "CREATE TABLE $mt_table_tf (id UInt64, value String, year UInt16) ENGINE = MergeTree() PARTITION BY year ORDER BY tuple()"

# Insert all data
# 4194304 is a number that came up during multiple iterations, it does not really mean anything (aside from the fact that the below numbers depend on it)
query "INSERT INTO $big_table SELECT number AS id, repeat('x', 100) AS data, 2025 AS year FROM numbers(4194304)"
query "INSERT INTO $big_table SELECT number AS id, repeat('x', 100) AS data, 2026 AS year FROM numbers(4194304)"
query "INSERT INTO $mt_table_tf VALUES (100, 'test1', 2022), (101, 'test2', 2022), (102, 'test3', 2023)"

# make sure we have only one part
query "OPTIMIZE TABLE $big_table FINAL"

# Get part names
big_part_max_bytes=$(query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$big_table' AND partition_id = '2025' AND active = 1 ORDER BY name LIMIT 1" | tr -d '\n')
big_part_max_rows=$(query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$big_table' AND partition_id = '2026' AND active = 1 ORDER BY name LIMIT 1" | tr -d '\n')

# ============================================================================
# ALL EXPORTS HAPPEN HERE
# ============================================================================

# this should generate ~4 files
query "ALTER TABLE $big_table EXPORT PART '$big_part_max_bytes' TO TABLE $big_destination_max_bytes SETTINGS allow_experimental_export_merge_tree_part = 1, export_merge_tree_part_max_bytes_per_file=3500000, output_format_parquet_row_group_size_bytes=1000000"
# export_merge_tree_part_max_rows_per_file = 1048576 (which is 4194304/4) to generate 4 files
query "ALTER TABLE $big_table EXPORT PART '$big_part_max_rows' TO TABLE $big_destination_max_rows SETTINGS allow_experimental_export_merge_tree_part = 1, export_merge_tree_part_max_rows_per_file=1048576"

echo "---- Table function with schema inheritance (no schema specified)"
query "ALTER TABLE $mt_table_tf EXPORT PART '2022_1_1_0' TO TABLE FUNCTION s3(s3_conn, filename='$tf_schema_inherit', format='Parquet', partition_strategy='hive') PARTITION BY year SETTINGS allow_experimental_export_merge_tree_part = 1"

echo "---- Table function with explicit compatible schema"
query "ALTER TABLE $mt_table_tf EXPORT PART '2023_2_2_0' TO TABLE FUNCTION s3(s3_conn, filename='$tf_schema_explicit', format='Parquet', structure='id UInt64, value String, year UInt16', partition_strategy='hive') PARTITION BY year SETTINGS allow_experimental_export_merge_tree_part = 1"

# ONE BIG SLEEP after all exports (longer because it writes multiple files)
sleep 20

# ============================================================================
# ALL SELECTS/VERIFICATIONS HAPPEN HERE
# ============================================================================

echo "---- Count files in big_destination_max_bytes, should be 5 (4 parquet, 1 commit)"
query "SELECT count(_file) FROM s3(s3_conn, filename='$big_destination_max_bytes/**', format='One')"

echo "---- Count rows in big_table and big_destination_max_bytes"
query "SELECT COUNT() from $big_table WHERE year = 2025"
query "SELECT COUNT() from $big_destination_max_bytes"

echo "---- Count files in big_destination_max_rows, should be 5 (4 parquet, 1 commit)"
query "SELECT count(_file) FROM s3(s3_conn, filename='$big_destination_max_rows/**', format='One')"

echo "---- Count rows in big_table and big_destination_max_rows"
query "SELECT COUNT() from $big_table WHERE year = 2026"
query "SELECT COUNT() from $big_destination_max_rows"

echo "---- Data should be exported with inherited schema"
query "SELECT * FROM s3(s3_conn, filename='$tf_schema_inherit/**.parquet') ORDER BY id"

echo "---- Data should be exported with explicit schema"
query "SELECT * FROM s3(s3_conn, filename='$tf_schema_explicit/**.parquet') ORDER BY id"

query "DROP TABLE IF EXISTS $big_table, $big_destination_max_bytes, $big_destination_max_rows, $mt_table_tf"
