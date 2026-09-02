#!/usr/bin/env bash
# Tags: zookeeper, no-object-storage

# Because REPLACE PARTITION does not forces immediate removal of replaced data parts from local filesystem
# (it tries to do it as quick as possible, but it still performed in separate thread asynchronously)
# and when we do DETACH TABLE / ATTACH TABLE or SYSTEM RESTART REPLICA, these files may be discovered
# and discarded after restart with Warning/Error messages in log. This is Ok.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS src;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS dst_r1;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS dst_r2;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE dst_r1 (p UInt64, k String, d UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst_1', '1') PARTITION BY p ORDER BY k
SETTINGS old_parts_lifetime=1, cleanup_delay_period=1, cleanup_delay_period_random_add=0, cleanup_thread_preferred_points_per_iteration=0;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE dst_r2 (p UInt64, k String, d UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst_1', '2') PARTITION BY p ORDER BY k
SETTINGS old_parts_lifetime=1, cleanup_delay_period=1, cleanup_delay_period_random_add=0, cleanup_thread_preferred_points_per_iteration=0;"

$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (0, '0', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (1, '0', 1), (1, '0', 3);"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (1, '0', 1), (1, '0', 3);"

$CLICKHOUSE_CLIENT --query="SELECT 'REPLACE empty partition with duplicated parts';"
$CLICKHOUSE_CLIENT --query="ALTER TABLE dst_r1 REPLACE PARTITION 1 FROM src;"
echo $?

$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA dst_r2;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r1;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst_r2;"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS src;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS dst_r1;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS dst_r2;"
