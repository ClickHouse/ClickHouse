#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CURDIR"/streaming.lib

opts=(
    "--allow_experimental_analyzer=1"
    "--allow_experimental_streaming=1"
)

$CLICKHOUSE_CLIENT "${opts[@]}" -q "DROP TABLE IF EXISTS t_streaming_test"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "CREATE TABLE t_streaming_test (a String, b UInt64) ENGINE = MergeTree() ORDER BY (a)"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES ('started', 0)"

echo "=== Test Streaming from subquery ==="

# start stream
read -r fifo_1 pid_1 < <(spawn $CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT a FROM (SELECT * FROM t_streaming_test STREAM)")
read_until "$fifo_1" "started"

# insert some data into stream
$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test (*) select number, number from numbers(1)"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test (*) select number, number from numbers(2)"
read_until "$fifo_1" "1"

# stop reading by killing client job
cleanup "$fifo_1" "$pid_1"

echo "=== Test Regular Union Stream ==="

$CLICKHOUSE_CLIENT "${opts[@]}" -q "TRUNCATE t_streaming_test"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES ('started', 0)"

# start stream
read -r fifo_1 pid_1 < <(spawn $CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT a FROM t_streaming_test UNION ALL SELECT a FROM t_streaming_test STREAM")
read_until "$fifo_1" "started"

# insert some data into stream
$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test (*) select number, number from numbers(1)"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test (*) select number, number from numbers(2)"
read_until "$fifo_1" "1"

# stop reading by killing client job
cleanup "$fifo_1" "$pid_1"

echo "=== Test Stream Union Stream ==="

$CLICKHOUSE_CLIENT "${opts[@]}" -q "TRUNCATE t_streaming_test"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES ('started', 0)"

# start stream
read -r fifo_1 pid_1 < <(spawn $CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT a FROM t_streaming_test STREAM WHERE b % 2 = 0 UNION ALL SELECT a FROM t_streaming_test STREAM WHERE b % 2 = 1")
read_until "$fifo_1" "started"

# insert some data into stream
$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES ('from-left', 0)"
read_until "$fifo_1" "from-left"

$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES ('from-right', 1)"
read_until "$fifo_1" "from-right"

# stop reading by killing client job
cleanup "$fifo_1" "$pid_1"
