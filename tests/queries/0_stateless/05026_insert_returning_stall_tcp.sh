#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# For `INSERT ... RETURNING` with the data sent by the client, the result is sent by another thread
# while the server still waits for the data. The server must not flush logs and ProfileEvents from
# the thread waiting for the data, otherwise they get into the middle of a result packet.
# The client stalls between blocks (the first block is emitted when the second row arrives), while
# `sleep_in_send_data_ms` keeps the result packet half-written during the stall.

$CLICKHOUSE_CLIENT -q "create table t_05026_insert_returning_stall (x UInt8) engine = Null"

(echo 1; sleep 0.5; echo 2; sleep 3; echo 3) | $CLICKHOUSE_CLIENT \
    --input_format_max_block_wait_ms 100 --input_format_connection_handling 1 \
    --min_insert_block_size_rows 0 --min_insert_block_size_bytes 0 --input_format_parallel_parsing 0 \
    -q "INSERT INTO t_05026_insert_returning_stall (x) RETURNING (SELECT x)
        SETTINGS sleep_in_send_data_ms = 1000, interactive_delay = 100000, async_insert = 0 FORMAT TSV"

$CLICKHOUSE_CLIENT -q "drop table t_05026_insert_returning_stall"
