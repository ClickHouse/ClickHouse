#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CURDIR"/streaming.lib

opts=(
    "--enable_streaming_queries=1"
)

insert_opts=(
    "--min_insert_block_size_rows=10"
    "--max_block_size=10"
)

$CLICKHOUSE_CLIENT "${opts[@]}" -q "DROP TABLE IF EXISTS t_streaming_test"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "CREATE TABLE t_streaming_test (a String, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS"
$CLICKHOUSE_CLIENT "${insert_opts[@]}" -q "INSERT INTO t_streaming_test select number, number from numbers(100)"

echo "=== Test Streaming cursor shift reading ==="

# start stream
read -r fifo_1 pid_1 < <(spawn $CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT toString(_block_number) || toString(_block_offset) FROM t_streaming_test STREAM CURSOR {'all': {'block_number': 8, 'block_offset': 5}}")

# 86 -> 109 (block 8 offsets 6..9; block 9 offsets 0..9; block 10 offsets 0..9)
read_until "$fifo_1" "109"

# stop reading by killing client job
cleanup "$fifo_1" "$pid_1"

$CLICKHOUSE_CLIENT "${opts[@]}" -q "DROP TABLE t_streaming_test"
