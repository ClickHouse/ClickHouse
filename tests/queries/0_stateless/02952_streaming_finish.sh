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
$CLICKHOUSE_CLIENT "${opts[@]}" -q "CREATE TABLE t_streaming_test (a String, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS queue_mode=1"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES ('0', 0), ('1', 1), ('2', 2), ('3', 3)"

echo "=== Test Streaming cancel after output finished from storage ==="

# start stream
$CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT count(*) FROM (SELECT * FROM t_streaming_test STREAM LIMIT 3)"

echo "=== Test Streaming cancel after output finished from subscription ==="

# start stream
# shellcheck disable=2034
read -r fifo_1 pid_1 < <(spawn $CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT count(*) FROM (SELECT * FROM t_streaming_test STREAM LIMIT 10)")

# open fifo for reading to extend it's lifetime
# shellcheck disable=2034
exec {fd}<>$fifo_1

# insert new data
$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test (*) SELECT number, number from numbers(10)"
read_until "$fifo_1" "10"
