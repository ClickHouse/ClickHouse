#!/usr/bin/env bash
# Tags: no-fasttest, replica, no-parallel, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

rmt_table="rmt_table_${RANDOM}"
s3_table="s3_table_${RANDOM}"
rmt_table_roundtrip="rmt_table_roundtrip_${RANDOM}"

query() {
    $CLICKHOUSE_CLIENT --query "$1"
}

query "DROP TABLE IF EXISTS $rmt_table, $s3_table, $rmt_table_roundtrip"

query "CREATE TABLE $rmt_table (id UInt64, year UInt16) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/$rmt_table', 'replica1') PARTITION BY year ORDER BY tuple()"
query "CREATE TABLE $s3_table (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='$s3_table', format=Parquet, partition_strategy='hive') PARTITION BY year"

query "INSERT INTO $rmt_table VALUES (1, 2020), (2, 2020), (4, 2021)"

query "INSERT INTO $rmt_table VALUES (3, 2020), (5, 2021)"

query "INSERT INTO $rmt_table VALUES (6, 2022), (7, 2022)"

# sync replicas
query "SYSTEM SYNC REPLICA $rmt_table"

query "ALTER TABLE $rmt_table EXPORT PARTITION ID '2020' TO TABLE $s3_table SETTINGS allow_experimental_export_merge_tree_part = 1"

query "ALTER TABLE $rmt_table EXPORT PARTITION ID '2021' TO TABLE $s3_table SETTINGS allow_experimental_export_merge_tree_part = 1"

# todo poll some kind of status
sleep 15

echo "Select from source table"
query "SELECT * FROM $rmt_table ORDER BY id"

echo "Select from destination table"
query "SELECT * FROM $s3_table ORDER BY id"

echo "Export partition 2022"
query "ALTER TABLE $rmt_table EXPORT PARTITION ID '2022' TO TABLE $s3_table SETTINGS allow_experimental_export_merge_tree_part = 1"

# todo poll some kind of status
sleep 5

echo "Select from destination table again"
query "SELECT * FROM $s3_table ORDER BY id"

query "CREATE TABLE $rmt_table_roundtrip ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/$rmt_table_roundtrip', 'replica1') PARTITION BY year ORDER BY tuple() AS SELECT * FROM $s3_table"

echo "---- Data in roundtrip ReplicatedMergeTree table (should match s3_table)"
query "SELECT * FROM $rmt_table_roundtrip ORDER BY id"

query "DROP TABLE IF EXISTS $rmt_table, $s3_table, $rmt_table_roundtrip"