#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./replication.lib
. "$CURDIR"/streaming.lib

opts=(
    "--allow_experimental_analyzer=1"
    "--allow_experimental_streaming=1"
)

function run_test
{
    echo "=== Test Single Streams ($1) ==="

    $CLICKHOUSE_CLIENT "${opts[@]}" -q "DROP TABLE IF EXISTS t_streaming_test"
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "CREATE TABLE t_streaming_test (a String, b UInt64) ENGINE = $1"
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES ('started', 0)"

    read -r fifo_1 pid_1 < <(spawn $CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT a, a || '-' || a, b * b FROM t_streaming_test STREAM")
    read_until "$fifo_1" "started"

    # insert some data into stream
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test (*) select number, number from numbers(5)"
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test (*) select number, number from numbers(7)"
    read_until "$fifo_1" "6-6"

    # stop reading by killing client job
    cleanup "$fifo_1" "$pid_1"

    echo "=== Test Multiple Streams ($1) ==="

    # clear table
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "DROP TABLE IF EXISTS t_streaming_test_2"
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "CREATE TABLE t_streaming_test_2 (a String, b UInt64) ENGINE = $1"
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test_2 VALUES ('started', 0) ('started', 1)"

    # spawn 2 streams
    read -r fifo_1 pid_1 < <(spawn $CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT a FROM t_streaming_test_2 STREAM WHERE b % 2 == 0")
    read -r fifo_2 pid_2 < <(spawn $CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT a FROM t_streaming_test_2 STREAM WHERE b % 2 == 1")
    read_until "$fifo_1" "started"
    read_until "$fifo_2" "started"

    # insert some data into stream
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test_2 (*) select 'same-x-2' as a, number as b from numbers(2)"
    read_until "$fifo_1" "same"
    read_until "$fifo_2" "same"

    # kill first stream
    cleanup "$fifo_2" "$pid_2"

    $CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test_2 (*) select 'only-for-1' as a, number as b from numbers(1)"
    read_until "$fifo_1" "only-for-1"

    # kill first stream
    cleanup "$fifo_1" "$pid_1"
}

for engine_type in 'MergeTree() ORDER BY (a)' 'TinyLog' 'StripeLog' 'Memory';
do
   run_test "$engine_type"
done
