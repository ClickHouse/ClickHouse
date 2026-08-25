#!/usr/bin/env bash
# Tags: replica, no-parallel, no-replicated-database, no-fasttest

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

# The source partitions by (year, country); the destination partitions by year only - a coarser key
# that is covered by the source partition key. Every source part has a single year, so it maps to
# exactly one destination partition and the unified plain-storage gate accepts the export even though
# the partition keys are not identical (this was rejected before the unification).
query "CREATE TABLE $rmt_table (id UInt64, year UInt16, country String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/$rmt_table', 'replica1') PARTITION BY (year, country) ORDER BY tuple()"
query "CREATE TABLE $s3_table (id UInt64, year UInt16, country String) ENGINE = S3(s3_conn, filename='$s3_table', format=Parquet, partition_strategy='hive') PARTITION BY year"

query "INSERT INTO $rmt_table VALUES (1, 2020, 'US'), (2, 2020, 'FR'), (3, 2021, 'US')"

echo "---- Export each source part into the coarser destination"
part_names=$(query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$rmt_table' AND active ORDER BY name")
for part in $part_names; do
    query "ALTER TABLE $rmt_table EXPORT PART '$part' TO TABLE $s3_table SETTINGS allow_experimental_export_merge_tree_part = 1"
done

echo "---- Destination should hold all rows"
query "SELECT * FROM $s3_table ORDER BY id"

echo "---- Round-trip back into a MergeTree table (should match the source)"
query "CREATE TABLE $rmt_table_roundtrip (id UInt64, year UInt16, country String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/$rmt_table_roundtrip', 'replica1') PARTITION BY (year, country) ORDER BY tuple()"
query "INSERT INTO $rmt_table_roundtrip SELECT * FROM $s3_table"
query "SELECT * FROM $rmt_table_roundtrip ORDER BY id"

query "DROP TABLE IF EXISTS $rmt_table, $s3_table, $rmt_table_roundtrip"
