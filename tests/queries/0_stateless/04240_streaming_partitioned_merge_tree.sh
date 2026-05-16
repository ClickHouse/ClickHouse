#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CURDIR"/streaming.lib

opts=(
    "--enable_streaming_queries=1"
)

$CLICKHOUSE_CLIENT "${opts[@]}" -q "DROP TABLE IF EXISTS t_streaming_test"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "CREATE TABLE t_streaming_test (p UInt8, a String) ENGINE = MergeTree ORDER BY a PARTITION BY p SETTINGS $STREAMING_TABLE_SETTINGS"

echo "=== Test Streaming from partitioned MergeTree ==="

# Two streams, each filtering its own partition.
read -r fifo_0 pid_0 < <(spawn $CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT a FROM t_streaming_test STREAM WHERE p = 0")
read -r fifo_1 pid_1 < <(spawn $CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT a FROM t_streaming_test STREAM WHERE p = 1")

$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES (0, 'p0-start')"
read_until "$fifo_0" "p0-start"

$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES (1, 'p1-start')"
read_until "$fifo_1" "p1-start"

$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES (0, 'p0-next')"
read_until "$fifo_0" "p0-next"

$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES (1, 'p1-next')"
read_until "$fifo_1" "p1-next"

# Cross-partition insert: each stream emits only its own row.
$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES (0, 'p0-last'), (1, 'p1-last')"
read_until "$fifo_0" "p0-last"
read_until "$fifo_1" "p1-last"

cleanup "$fifo_0" "$pid_0"
cleanup "$fifo_1" "$pid_1"

$CLICKHOUSE_CLIENT "${opts[@]}" -q "DROP TABLE t_streaming_test"
