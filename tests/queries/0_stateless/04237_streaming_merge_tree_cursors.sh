#!/usr/bin/env bash
# Tags: long, no-shared-merge-tree, no-random-detach

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CURDIR"/streaming.lib


insert_opts=(
    "--min_insert_block_size_rows=10"
    "--max_block_size=10"
)

$STREAMING_CLIENT -q "DROP TABLE IF EXISTS t_streaming_test"
$STREAMING_CLIENT -q "CREATE TABLE t_streaming_test (a String, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS"

# start stream
read -r fifo_1 pid_1 < <(spawn $STREAMING_CLIENT -q "SELECT toString(_block_number) || toString(_block_offset) FROM t_streaming_test STREAM CURSOR {'all': {'block_number': 8, 'block_offset': 5}}")

echo "=== Test Streaming cursor shift reading ==="

# 86 -> 109 (block 8 offsets 6..9; block 9 offsets 0..9; block 10 offsets 0..9)
$CLICKHOUSE_CLIENT "${insert_opts[@]}" -q "INSERT INTO t_streaming_test select number, number from numbers(100)" &
read_until "$fifo_1" "109"

# stop reading by killing client job
cleanup "$fifo_1" "$pid_1"

$STREAMING_CLIENT -q "DROP TABLE t_streaming_test"
