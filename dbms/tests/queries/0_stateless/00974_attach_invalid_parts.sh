#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

ch_dir=`${CLICKHOUSE_EXTRACT_CONFIG} -k path`
cur_db=`${CLICKHOUSE_CLIENT} --query "SELECT currentDatabase()"`

echo '=== cannot attach active ===';
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS attach_active";
$CLICKHOUSE_CLIENT --query="CREATE TABLE attach_active (n UInt64) ENGINE = MergeTree() PARTITION BY intDiv(n, 4) ORDER BY n";
$CLICKHOUSE_CLIENT --query="INSERT INTO attach_active SELECT number FROM system.numbers LIMIT 16";
$CLICKHOUSE_CLIENT --query="ALTER TABLE attach_active ATTACH PART '../1_2_2_0'" 2>&1 | grep "Invalid part name" > /dev/null && echo 'OK'
$CLICKHOUSE_CLIENT --query="SElECT name FROM system.parts WHERE table='attach_active' AND database='${cur_db}' ORDER BY name FORMAT TSV";
$CLICKHOUSE_CLIENT --query="SElECT count(), sum(n) FROM attach_active FORMAT TSV";
$CLICKHOUSE_CLIENT --query="DROP TABLE attach_active";



echo '=== attach all valid parts ===';
$CLICKHOUSE_CLIENT --query="SYSTEM STOP MERGES";
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS attach_partitions";
$CLICKHOUSE_CLIENT --query="CREATE TABLE attach_partitions (n UInt64) ENGINE = MergeTree() PARTITION BY intDiv(n, 8) ORDER BY n";
$CLICKHOUSE_CLIENT --query="INSERT INTO attach_partitions SELECT number FROM system.numbers WHERE number % 2 = 0 LIMIT 8";
$CLICKHOUSE_CLIENT --query="INSERT INTO attach_partitions SELECT number FROM system.numbers WHERE number % 2 = 1 LIMIT 8";

$CLICKHOUSE_CLIENT --query="ALTER TABLE attach_partitions DETACH PARTITION 0";
mkdir $ch_dir/data/$cur_db/attach_partitions/detached/0_5_5_0/             # broken part
cp -r $ch_dir/data/$cur_db/attach_partitions/detached/0_1_1_0/ $ch_dir/data/$cur_db/attach_partitions/detached/attaching_0_6_6_0/
cp -r $ch_dir/data/$cur_db/attach_partitions/detached/0_3_3_0/ $ch_dir/data/$cur_db/attach_partitions/detached/delete_tmp_0_7_7_0/
$CLICKHOUSE_CLIENT --query="ALTER TABLE attach_partitions ATTACH PARTITION 0";

$CLICKHOUSE_CLIENT --query="SElECT name FROM system.parts WHERE table='attach_partitions' AND database='${cur_db}' ORDER BY name FORMAT TSV";
$CLICKHOUSE_CLIENT --query="SElECT count(), sum(n) FROM attach_partitions FORMAT TSV";
echo '=== detached ===';
$CLICKHOUSE_CLIENT --query="SELECT directory_name FROM system.detached_parts WHERE table='attach_partitions' AND database='${cur_db}' FORMAT TSV";

$CLICKHOUSE_CLIENT --query="DROP TABLE attach_partitions";
$CLICKHOUSE_CLIENT --query="SYSTEM START MERGES";
