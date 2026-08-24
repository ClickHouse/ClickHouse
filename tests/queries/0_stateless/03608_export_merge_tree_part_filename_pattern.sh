#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires s3 storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

R=$RANDOM
mt="mt_${R}"
dest1="fp_dest1_${R}"
dest2="fp_dest2_${R}"
dest3="fp_dest3_${R}"

query() {
    $CLICKHOUSE_CLIENT --query "$1"
}

query "DROP TABLE IF EXISTS $mt, $dest1, $dest2, $dest3"

query "CREATE TABLE $mt (id UInt64, year UInt16) ENGINE = MergeTree() PARTITION BY year ORDER BY tuple()"
query "INSERT INTO $mt VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021)"

query "CREATE TABLE $dest1 (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='$dest1', format=Parquet, partition_strategy='hive') PARTITION BY year"
query "CREATE TABLE $dest2 (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='$dest2', format=Parquet, partition_strategy='hive') PARTITION BY year"
query "CREATE TABLE $dest3 (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='$dest3', format=Parquet, partition_strategy='hive') PARTITION BY year"

echo "---- Test: Default pattern {part_name}_{checksum}"
query "ALTER TABLE $mt EXPORT PART '2020_1_1_0' TO TABLE $dest1 SETTINGS allow_experimental_export_merge_tree_part = 1, export_merge_tree_part_filename_pattern = '{part_name}_{checksum}'"
sleep 3
query "SELECT * FROM $dest1 ORDER BY id"
echo "---- Verify filename matches 2020_1_1_0_*.1.parquet"
query "SELECT count() FROM s3(s3_conn, filename='$dest1/**/2020_1_1_0_*.1.parquet', format='One')"

echo "---- Test: Custom prefix pattern"
query "ALTER TABLE $mt EXPORT PART '2021_2_2_0' TO TABLE $dest2 SETTINGS allow_experimental_export_merge_tree_part = 1, export_merge_tree_part_filename_pattern = 'myprefix_{part_name}'"
sleep 3
query "SELECT * FROM $dest2 ORDER BY id"
echo "---- Verify filename matches myprefix_2021_2_2_0.1.parquet"
query "SELECT count() FROM s3(s3_conn, filename='$dest2/**/myprefix_2021_2_2_0.1.parquet', format='One')"

echo "---- Test: Pattern with macros"
query "ALTER TABLE $mt EXPORT PART '2020_1_1_0' TO TABLE $dest3 SETTINGS allow_experimental_export_merge_tree_part = 1, export_merge_tree_part_filename_pattern = '{database}_{table}_{part_name}'"
sleep 3
query "SELECT * FROM $dest3 ORDER BY id"
echo "---- Verify macros expanded (no literal braces in parquet filenames, that's the best we can do for stateless tests)"
query "SELECT count() = 0 FROM s3(s3_conn, filename='$dest3/**/*.1.parquet', format='One') WHERE _file LIKE '%{%'"

query "DROP TABLE IF EXISTS $mt, $dest1, $dest2, $dest3"
