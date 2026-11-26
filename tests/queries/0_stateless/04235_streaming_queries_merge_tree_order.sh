#!/usr/bin/env bash
# Tags: long, no-shared-merge-tree

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CURDIR"/streaming.lib


insert_opts=(
    "--min_insert_block_size_rows=1"
    "--max_block_size=1"
)

$STREAMING_CLIENT -q "DROP TABLE IF EXISTS t_streaming_test"
$STREAMING_CLIENT -q "CREATE TABLE t_streaming_test (a String, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS"

echo "=== Test Streaming strict fifo order check ==="

echo "start parallel insert"
$CLICKHOUSE_CLIENT "${insert_opts[@]}" -q "INSERT INTO t_streaming_test select number, number from numbers(100)" &
$CLICKHOUSE_CLIENT "${insert_opts[@]}" -q "INSERT INTO t_streaming_test select number, number from numbers(100)" &

echo "start stream"
read -r fifo_1 pid_1 < <(spawn $STREAMING_CLIENT -q "SELECT _block_number FROM t_streaming_test STREAM")

echo "start parallel insert"
$CLICKHOUSE_CLIENT "${insert_opts[@]}" -q "INSERT INTO t_streaming_test select number, number from numbers(100)" &
$CLICKHOUSE_CLIENT "${insert_opts[@]}" -q "INSERT INTO t_streaming_test select number, number from numbers(100)" &

read_until "$fifo_1" "400"

# stop reading by killing client job
cleanup "$fifo_1" "$pid_1"

wait

$STREAMING_CLIENT -q "DROP TABLE t_streaming_test"
