#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CURDIR"/streaming.lib


$STREAMING_CLIENT -q "DROP TABLE IF EXISTS t_streaming_test"
$STREAMING_CLIENT -q "CREATE TABLE t_streaming_test (a String, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS"
$STREAMING_CLIENT -q "INSERT INTO t_streaming_test VALUES ('started', 0)"

echo "=== Test Streaming from subquery ==="

read -r fifo_1 pid_1 < <(spawn $STREAMING_CLIENT -q "SELECT a FROM (SELECT * FROM t_streaming_test STREAM)")
read_until "$fifo_1" "started"

$STREAMING_CLIENT -q "INSERT INTO t_streaming_test select number, number from numbers(1)"
$STREAMING_CLIENT -q "INSERT INTO t_streaming_test select number, number from numbers(2)"
read_until "$fifo_1" "1"

cleanup "$fifo_1" "$pid_1"

echo "=== Test Regular Union Stream ==="

$STREAMING_CLIENT -q "TRUNCATE t_streaming_test"
$STREAMING_CLIENT -q "INSERT INTO t_streaming_test VALUES ('started', 0)"

read -r fifo_1 pid_1 < <(spawn $STREAMING_CLIENT -q "SELECT a FROM t_streaming_test UNION ALL SELECT a FROM t_streaming_test STREAM")
read_until "$fifo_1" "started"

$STREAMING_CLIENT -q "INSERT INTO t_streaming_test select number, number from numbers(1)"
$STREAMING_CLIENT -q "INSERT INTO t_streaming_test select number, number from numbers(2)"
read_until "$fifo_1" "1"

cleanup "$fifo_1" "$pid_1"

echo "=== Test Stream Union Stream ==="

$STREAMING_CLIENT -q "TRUNCATE t_streaming_test"
$STREAMING_CLIENT -q "INSERT INTO t_streaming_test VALUES ('started', 0)"

read -r fifo_1 pid_1 < <(spawn $STREAMING_CLIENT -q "SELECT a FROM t_streaming_test STREAM WHERE b % 2 = 0 UNION ALL SELECT a FROM t_streaming_test STREAM WHERE b % 2 = 1")
read_until "$fifo_1" "started"

$STREAMING_CLIENT -q "INSERT INTO t_streaming_test VALUES ('from-left', 0)"
read_until "$fifo_1" "from-left"

$STREAMING_CLIENT -q "INSERT INTO t_streaming_test VALUES ('from-right', 1)"
read_until "$fifo_1" "from-right"

cleanup "$fifo_1" "$pid_1"

$STREAMING_CLIENT -q "DROP TABLE t_streaming_test"
