#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CURDIR"/streaming.lib


echo "=== Test Single Streams ==="

$STREAMING_CLIENT -q "DROP TABLE IF EXISTS t_streaming_test"
$STREAMING_CLIENT -q "CREATE TABLE t_streaming_test (a String, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS"
$STREAMING_CLIENT -q "INSERT INTO t_streaming_test VALUES ('started', 0)"

read -r fifo_1 pid_1 < <(spawn $STREAMING_CLIENT -q "SELECT a, a || '-' || a, b * b FROM t_streaming_test STREAM")
read_until "$fifo_1" "started"

# insert some data into stream
$STREAMING_CLIENT -q "INSERT INTO t_streaming_test select number, number from numbers(5)"
$STREAMING_CLIENT -q "INSERT INTO t_streaming_test select number, number from numbers(7)"
read_until "$fifo_1" "6-6"

# stop reading by killing client job
cleanup "$fifo_1" "$pid_1"

echo "=== Test Multiple Streams ==="

$STREAMING_CLIENT -q "DROP TABLE IF EXISTS t_streaming_test_2"
$STREAMING_CLIENT -q "CREATE TABLE t_streaming_test_2 (a String, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS"
$STREAMING_CLIENT -q "INSERT INTO t_streaming_test_2 VALUES ('started', 0) ('started', 1)"

# spawn 2 streams
read -r fifo_1 pid_1 < <(spawn $STREAMING_CLIENT -q "SELECT a FROM t_streaming_test_2 STREAM WHERE b % 2 == 0")
read -r fifo_2 pid_2 < <(spawn $STREAMING_CLIENT -q "SELECT a FROM t_streaming_test_2 STREAM WHERE b % 2 == 1")
read_until "$fifo_1" "started"
read_until "$fifo_2" "started"

# insert some data into stream
$STREAMING_CLIENT -q "INSERT INTO t_streaming_test_2 select 'same-x-2' as a, number as b from numbers(2)"
read_until "$fifo_1" "same"
read_until "$fifo_2" "same"

# kill second stream
cleanup "$fifo_2" "$pid_2"

$STREAMING_CLIENT -q "INSERT INTO t_streaming_test_2 select 'only-for-1' as a, number as b from numbers(1)"
read_until "$fifo_1" "only-for-1"

# kill first stream
cleanup "$fifo_1" "$pid_1"

$STREAMING_CLIENT -q "DROP TABLE t_streaming_test"
$STREAMING_CLIENT -q "DROP TABLE t_streaming_test_2"
