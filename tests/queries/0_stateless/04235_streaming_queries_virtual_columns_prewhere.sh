#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CURDIR"/streaming.lib

$STREAMING_CLIENT -q "DROP TABLE IF EXISTS t_streaming_virtuals_prewhere"
$STREAMING_CLIENT -q "CREATE TABLE t_streaming_virtuals_prewhere (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS"
$STREAMING_CLIENT -q "INSERT INTO t_streaming_virtuals_prewhere VALUES (42)"

# Filter by _partition_id, _block_number and _block_offset must not break internal cursor calculation.
read -r fifo_1 pid_1 < <(spawn $STREAMING_CLIENT -q "SELECT a FROM t_streaming_virtuals_prewhere STREAM WHERE _partition_id = 'all' AND _block_number = 1 AND _block_offset = 0")
read_until "$fifo_1" "42"

cleanup "$fifo_1" "$pid_1"
