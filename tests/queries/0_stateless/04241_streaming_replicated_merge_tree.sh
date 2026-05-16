#!/usr/bin/env bash
# Tags: long, zookeeper, no-shared-merge-tree

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CURDIR"/streaming.lib


$STREAMING_CLIENT -q "DROP TABLE IF EXISTS t_streaming_test_r1"
$STREAMING_CLIENT -q "DROP TABLE IF EXISTS t_streaming_test_r2"
$STREAMING_CLIENT -q "CREATE TABLE t_streaming_test_r1 (p UInt8, a String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04241/t_streaming_test', 'r1') ORDER BY a PARTITION BY p SETTINGS $STREAMING_TABLE_SETTINGS"
$STREAMING_CLIENT -q "CREATE TABLE t_streaming_test_r2 (p UInt8, a String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04241/t_streaming_test', 'r2') ORDER BY a PARTITION BY p SETTINGS $STREAMING_TABLE_SETTINGS"

echo "=== Test Streaming from partitioned ReplicatedMergeTree ==="

# Stream partition 0 from r1, partition 1 from r2.
# Block numbers are assigned per partition by ZooKeeper, so each stream sees
# its own partition's commit-order sequence regardless of which replica inserted.
read -r fifo_0 pid_0 < <(spawn $STREAMING_CLIENT -q "SELECT _block_number, a FROM t_streaming_test_r1 STREAM WHERE p = 0")
read -r fifo_1 pid_1 < <(spawn $STREAMING_CLIENT -q "SELECT _block_number, a FROM t_streaming_test_r2 STREAM WHERE p = 1")

$STREAMING_CLIENT -q "INSERT INTO t_streaming_test_r1 VALUES (0, 'p0-from-r1')"
read_until "$fifo_0" "p0-from-r1"

$STREAMING_CLIENT -q "INSERT INTO t_streaming_test_r2 VALUES (1, 'p1-from-r2')"
read_until "$fifo_1" "p1-from-r2"

$STREAMING_CLIENT -q "INSERT INTO t_streaming_test_r2 VALUES (0, 'p0-from-r2')"
read_until "$fifo_0" "p0-from-r2"

$STREAMING_CLIENT -q "INSERT INTO t_streaming_test_r1 VALUES (1, 'p1-from-r1')"
read_until "$fifo_1" "p1-from-r1"

cleanup "$fifo_0" "$pid_0"
cleanup "$fifo_1" "$pid_1"

$STREAMING_CLIENT -q "DROP TABLE t_streaming_test_r1 SYNC"
$STREAMING_CLIENT -q "DROP TABLE t_streaming_test_r2 SYNC"
