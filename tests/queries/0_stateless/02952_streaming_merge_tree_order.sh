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

insert_opts=(
    "--min_insert_block_size_rows=1"
    "--max_block_size=1"
)

$CLICKHOUSE_CLIENT "${opts[@]}" -q "DROP TABLE IF EXISTS t_streaming_test"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "CREATE TABLE t_streaming_test (a String, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS queue_mode=1"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES ('started', 0)"

echo "=== Test Streaming strict fifo order check ==="

# start stream
read -r fifo_1 pid_1 < <(spawn $CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT _queue_block_number FROM t_streaming_test STREAM")
read_until "$fifo_1" "1"

echo "start parallel insert"
$CLICKHOUSE_CLIENT "${insert_opts[@]}" -q "INSERT INTO t_streaming_test (*) select number, number from numbers(100)" &
$CLICKHOUSE_CLIENT "${insert_opts[@]}" -q "INSERT INTO t_streaming_test (*) select number, number from numbers(100)" &

read_until "$fifo_1" "201"

# stop reading by killing client job
cleanup "$fifo_1" "$pid_1"
