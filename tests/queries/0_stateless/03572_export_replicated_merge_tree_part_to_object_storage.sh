#!/usr/bin/env bash
# Tags: replica, no-parallel, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

rmt_table="rmt_table_${RANDOM}"
s3_table="s3_table_${RANDOM}"

query() {
    $CLICKHOUSE_CLIENT --query "$1"
}

query "DROP TABLE IF EXISTS $rmt_table, $s3_table"

query "CREATE TABLE $rmt_table (id UInt64, year UInt16) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/$rmt_table', 'replica1') PARTITION BY year ORDER BY tuple()"
query "CREATE TABLE $s3_table (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='$s3_table', format=Parquet, partition_strategy='hive') PARTITION BY year"

query "INSERT INTO $rmt_table VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021)"

echo "---- Get actual part names and export them"
part_2020=$(query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$rmt_table' AND partition = '2020' ORDER BY name LIMIT 1" | tr -d '\n')
part_2021=$(query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$rmt_table' AND partition = '2021' ORDER BY name LIMIT 1" | tr -d '\n')

query "ALTER TABLE $rmt_table EXPORT PART '$part_2020' TO TABLE $s3_table SETTINGS allow_experimental_export_merge_tree_part = 1"
query "ALTER TABLE $rmt_table EXPORT PART '$part_2021' TO TABLE $s3_table SETTINGS allow_experimental_export_merge_tree_part = 1"

echo "---- Both data parts should appear"
query "SELECT * FROM $s3_table ORDER BY id"

echo "---- Export the same part again, it should be idempotent"
query "ALTER TABLE $rmt_table EXPORT PART '$part_2020' TO TABLE $s3_table SETTINGS allow_experimental_export_merge_tree_part = 1"

query "SELECT * FROM $s3_table ORDER BY id"

query "DROP TABLE IF EXISTS $rmt_table, $s3_table"
