#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

opts=(
    "--allow_experimental_analyzer=1"
    "--allow_experimental_streaming=1"
)

function run_test {
    printf "=====\n"
    printf "running test for engine: $1\n"
    sleep 0.5

    $CLICKHOUSE_CLIENT "${opts[@]}" -q "DROP TABLE IF EXISTS t_streaming_test"
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "CREATE TABLE t_streaming_test (a String, b UInt64) ENGINE = $1"

    $CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT count() FROM t_streaming_test"
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES ('a', 100)"
    sleep 0.5

    # spawn stream
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT a, a || '-' || a, b * b FROM t_streaming_test STREAM" &
    sleep 0.5

    # insert some data into stream
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test (*) select number as a, number as b from numbers(7)"
    sleep 0.5

    # stop reading by killing client job
    pkill -P $$
    sleep 0.5

    # clear table
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "TRUNCATE t_streaming_test"

    # spawn 2 streams
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT a FROM t_streaming_test STREAM WHERE b % 2 == 0" &
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT a FROM t_streaming_test STREAM WHERE b % 2 == 1" &
    sleep 0.5

    # insert some data into stream
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test (*) select 'same-x-2' as a, number as b from numbers(2)"
    sleep 0.5

    # stop reading by killing client job
    pkill -P $$
    sleep 0.5
}

for engine_type in 'MergeTree() ORDER BY (a)' 'TinyLog' 'StripeLog' 'Memory';
do
   run_test "$engine_type"
done


