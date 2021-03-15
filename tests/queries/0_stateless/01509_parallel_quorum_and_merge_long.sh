#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS parallel_q1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS parallel_q2"


$CLICKHOUSE_CLIENT -q "CREATE TABLE parallel_q1 (x UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/parallel_q', 'r1') ORDER BY tuple() SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 0, cleanup_delay_period_random_add = 0"

$CLICKHOUSE_CLIENT -q "CREATE TABLE parallel_q2 (x UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/parallel_q', 'r2') ORDER BY tuple() SETTINGS always_fetch_merged_part = 1"

$CLICKHOUSE_CLIENT -q "SYSTEM STOP REPLICATION QUEUES parallel_q2"

$CLICKHOUSE_CLIENT -q "INSERT INTO parallel_q1 VALUES (1)"

$CLICKHOUSE_CLIENT --insert_quorum 2 --insert_quorum_parallel 1 --query="INSERT INTO parallel_q1 VALUES (2)" &

part_count=$($CLICKHOUSE_CLIENT --query="SELECT COUNT() FROM system.parts WHERE table='parallel_q1' and database='${CLICKHOUSE_DATABASE}'")

# Check part inserted locally
while [[ $part_count != 2 ]]
do
    sleep 0.1
    part_count=$($CLICKHOUSE_CLIENT --query="SELECT COUNT() FROM system.parts WHERE table='parallel_q1' and database='${CLICKHOUSE_DATABASE}'")
done

$CLICKHOUSE_CLIENT --replication_alter_partitions_sync 0 -q "OPTIMIZE TABLE parallel_q1 FINAL"

# check part merged locally
has_part=$($CLICKHOUSE_CLIENT --query="SELECT COUNT() FROM system.parts WHERE table='parallel_q1' and database='${CLICKHOUSE_DATABASE}' and name='all_0_1_1'")

while [[ $has_part != 1 ]]
do
    sleep 0.1
    has_part=$($CLICKHOUSE_CLIENT --query="SELECT COUNT() FROM system.parts WHERE table='parallel_q1' and database='${CLICKHOUSE_DATABASE}' and name='all_0_1_1'")
done

# check source parts removed locally
active_parts_count=$($CLICKHOUSE_CLIENT --query="SELECT COUNT() FROM system.parts WHERE table='parallel_q1' and database='${CLICKHOUSE_DATABASE}' and active=1")

while [[ $active_parts_count != 1 ]]
do
    sleep 0.1
    active_parts_count=$($CLICKHOUSE_CLIENT --query="SELECT COUNT() FROM system.parts WHERE table='parallel_q1' and database='${CLICKHOUSE_DATABASE}'")
done

# download merged part
$CLICKHOUSE_CLIENT -q "SYSTEM START REPLICATION QUEUES parallel_q2"

$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA parallel_q2"

# quorum satisfied even for merged part
wait

$CLICKHOUSE_CLIENT --query="SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT --query="SELECT name FROM system.parts WHERE table='parallel_q2' and database='${CLICKHOUSE_DATABASE}' and active=1 ORDER BY name"
$CLICKHOUSE_CLIENT --query="SELECT event_type FROM system.part_log WHERE table='parallel_q2' and database='${CLICKHOUSE_DATABASE}' and part_name='all_0_1_1'"
$CLICKHOUSE_CLIENT --query="SELECT COUNT() FROM parallel_q2"
$CLICKHOUSE_CLIENT --query="SELECT COUNT() FROM parallel_q1"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS parallel_q1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS parallel_q2"
