#!/usr/bin/env bash

# Because REPLACE PARTITION does not forces immediate removal of replaced data parts from local filesystem
# (it tries to do it as quick as possible, but it still performed in separate thread asynchronously)
# and when we do DETACH TABLE / ATTACH TABLE or SYSTEM RESTART REPLICA, these files may be discovered
# and discarded after restart with Warning/Error messages in log. This is Ok.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

function query_with_retry
{
    retry=0
    until [ $retry -ge 5 ]
    do
        result=$($CLICKHOUSE_CLIENT $2 --query="$1" 2>&1)
        if [ "$?" == 0 ]; then
            echo -n "$result"
            return
        else
            retry=$(($retry + 1))
            sleep 3
        fi
    done
    echo "Query '$1' failed with '$result'"
}

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS src;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS dst_r1;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS dst_r2;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE dst_r1 (p UInt64, k String, d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test_00626/dst_1', '1') PARTITION BY p ORDER BY k SETTINGS old_parts_lifetime=1, cleanup_delay_period=1, cleanup_delay_period_random_add=0;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE dst_r2 (p UInt64, k String, d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test_00626/dst_1', '2') PARTITION BY p ORDER BY k SETTINGS old_parts_lifetime=1, cleanup_delay_period=1, cleanup_delay_period_random_add=0;"

$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (0, '0', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (1, '0', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (1, '1', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (2, '0', 1);"

$CLICKHOUSE_CLIENT --query="SELECT 'Initial';"
$CLICKHOUSE_CLIENT --query="INSERT INTO dst_r1 VALUES (0, '1', 2);"
$CLICKHOUSE_CLIENT --query="INSERT INTO dst_r1 VALUES (1, '1', 2), (1, '2', 2);"
$CLICKHOUSE_CLIENT --query="INSERT INTO dst_r1 VALUES (2, '1', 2);"

$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA dst_r2;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM src;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r1;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r2;"


$CLICKHOUSE_CLIENT --query="SELECT 'REPLACE simple';"
query_with_retry "ALTER TABLE dst_r1 REPLACE PARTITION 1 FROM src;"
query_with_retry "ALTER TABLE src DROP PARTITION 1;"

$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA dst_r2;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM src;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r1;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r2;"


$CLICKHOUSE_CLIENT --query="SELECT 'REPLACE empty';"
query_with_retry "ALTER TABLE src DROP PARTITION 1;"
query_with_retry "ALTER TABLE dst_r1 REPLACE PARTITION 1 FROM src;"

$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA dst_r2;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r1;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r2;"


$CLICKHOUSE_CLIENT --query="SELECT 'REPLACE recursive';"
query_with_retry "ALTER TABLE dst_r1 DROP PARTITION 1;"
$CLICKHOUSE_CLIENT --query="INSERT INTO dst_r1 VALUES (1, '1', 2), (1, '2', 2);"

$CLICKHOUSE_CLIENT --query="CREATE table test_block_numbers (m UInt64) ENGINE MergeTree() ORDER BY tuple();"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_block_numbers SELECT max(max_block_number) AS m FROM system.parts WHERE database='$CLICKHOUSE_DATABASE' AND  table='dst_r1' AND active AND name LIKE '1_%';"

query_with_retry "ALTER TABLE dst_r1 REPLACE PARTITION 1 FROM dst_r1;"
$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA dst_r2;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r1;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r2;"

$CLICKHOUSE_CLIENT --query="INSERT INTO test_block_numbers SELECT max(max_block_number) AS m FROM system.parts WHERE database='$CLICKHOUSE_DATABASE' AND  table='dst_r1' AND active AND name LIKE '1_%';"
$CLICKHOUSE_CLIENT --query="SELECT (max(m) - min(m) > 1) AS new_block_is_generated FROM test_block_numbers;"
$CLICKHOUSE_CLIENT --query="DROP TABLE test_block_numbers;"


$CLICKHOUSE_CLIENT --query="SELECT 'ATTACH FROM';"
query_with_retry "ALTER TABLE dst_r1 DROP PARTITION 1;"
$CLICKHOUSE_CLIENT --query="DROP TABLE src;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (1, '0', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (1, '1', 1);"

$CLICKHOUSE_CLIENT --query="INSERT INTO dst_r2 VALUES (1, '1', 2);"
query_with_retry "ALTER TABLE dst_r2 ATTACH PARTITION 1 FROM src;"

$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA dst_r1;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r1;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r2;"


$CLICKHOUSE_CLIENT --query="SELECT 'REPLACE with fetch';"
$CLICKHOUSE_CLIENT --query="DROP TABLE src;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (1, '0', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (1, '1', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO dst_r1 VALUES (1, '1', 2); -- trash part to be deleted"

# Stop replication at the second replica and remove source table to use fetch instead of copying
$CLICKHOUSE_CLIENT --query="SYSTEM STOP REPLICATION QUEUES dst_r2;"
query_with_retry "ALTER TABLE dst_r1 REPLACE PARTITION 1 FROM src;"
$CLICKHOUSE_CLIENT --query="DROP TABLE src;"
$CLICKHOUSE_CLIENT --query="SYSTEM START REPLICATION QUEUES dst_r2;"

$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA dst_r2;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r1;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r2;"


$CLICKHOUSE_CLIENT --query="SELECT 'REPLACE with fetch of merged';"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS src;"
query_with_retry "ALTER TABLE dst_r1 DROP PARTITION 1;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (1, '0', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (1, '1', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO dst_r1 VALUES (1, '1', 2); -- trash part to be deleted"

$CLICKHOUSE_CLIENT --query="SYSTEM STOP MERGES dst_r2;"
$CLICKHOUSE_CLIENT --query="SYSTEM STOP REPLICATION QUEUES dst_r2;"
query_with_retry "ALTER TABLE dst_r1 REPLACE PARTITION 1 FROM src;"
$CLICKHOUSE_CLIENT --query="DROP TABLE src;"

# do not wait other replicas to execute OPTIMIZE

$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d), uniqExact(_part) FROM dst_r1;"
$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA dst_r1;"
query_with_retry "OPTIMIZE TABLE dst_r1 PARTITION 1;" "--replication_alter_partitions_sync=0 --optimize_throw_if_noop=1"

$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA dst_r1;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d), uniqExact(_part) FROM dst_r1;"

$CLICKHOUSE_CLIENT --query="SYSTEM START REPLICATION QUEUES dst_r2;"
$CLICKHOUSE_CLIENT --query="SYSTEM START MERGES dst_r2;"
$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA dst_r2;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d), uniqExact(_part) FROM dst_r2;"

$CLICKHOUSE_CLIENT --query="SELECT 'After restart';"
$CLICKHOUSE_CLIENT --query="SYSTEM RESTART REPLICA dst_r1;"
$CLICKHOUSE_CLIENT --query="SYSTEM RESTART REPLICAS;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r1;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r2;"

$CLICKHOUSE_CLIENT --query="SELECT 'DETACH+ATTACH PARTITION';"
query_with_retry "ALTER TABLE dst_r1 DETACH PARTITION 0;"
query_with_retry "ALTER TABLE dst_r1 DETACH PARTITION 1;"
query_with_retry "ALTER TABLE dst_r1 DETACH PARTITION 2;"
query_with_retry "ALTER TABLE dst_r1 ATTACH PARTITION 1;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r1;"
$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA dst_r2;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r2;"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS src;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS dst_r1;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS dst_r2;"
