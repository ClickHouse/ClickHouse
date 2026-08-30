#!/usr/bin/env bash
# Tags: long, no-shared-merge-tree

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

echo "=== Test Streaming cursor shift reading ==="

# Prime the partition (block 1): a cursor of a partition that is not assigned yet is evicted.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_streaming_test VALUES ('0', 0)"

# start stream
read -r fifo_1 pid_1 < <(spawn $STREAMING_CLIENT -q "SELECT toString(_block_number) || toString(_block_offset) FROM t_streaming_test STREAM CURSOR {'all': {'block_number': 8, 'block_offset': 5}}")

# 86 -> 109 (the insert fills blocks 2..11; block 8 offsets 6..9, blocks 9 and 10, the tail of block 11 is not awaited)
$CLICKHOUSE_CLIENT "${insert_opts[@]}" -q "INSERT INTO t_streaming_test select number, number from numbers(100)" &
read_until "$fifo_1" "109"

# stop reading by killing client job
cleanup "$fifo_1" "$pid_1"

$STREAMING_CLIENT -q "DROP TABLE t_streaming_test"
