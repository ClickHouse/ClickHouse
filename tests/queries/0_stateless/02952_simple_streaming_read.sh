#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

opts=(
    "--allow_experimental_analyzer=1"
    "--allow_experimental_streaming=1"
)

$CLICKHOUSE_CLIENT "${opts[@]}" -q "DROP TABLE IF EXISTS t_streaming_test"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "CREATE TABLE t_streaming_test (a String, b UInt64) ENGINE = MergeTree ORDER BY (a)"

$CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT count() FROM t_streaming_test"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES ('a', 100)"

# spawn stream
$CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT a, a || '-' || a, b * b FROM t_streaming_test STREAM" &

# insert some data into stream
$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test (*) select number as a, number as b from numbers(7)"

# stop reading by killing client job
sleep 0.5
pkill -P $$

# clear table
$CLICKHOUSE_CLIENT "${opts[@]}" -q "TRUNCATE t_streaming_test"

# spawn 2 streams
$CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT a FROM t_streaming_test STREAM" &
$CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT a FROM t_streaming_test STREAM" &

# insert some data into stream
$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test (*) select 'same-x-2' as a, number as b from numbers(2)"

# stop reading by killing client job
sleep 0.5
pkill -P $$
