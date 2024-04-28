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

function run_test
{
    echo "=== Alter test ($1) ==="

    $CLICKHOUSE_CLIENT "${opts[@]}" -q "DROP TABLE IF EXISTS t_streaming_test"
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "CREATE TABLE t_streaming_test (a String, b UInt64) ENGINE = $1"
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES ('started', 0)"

    # start stream
    # shellcheck disable=2034
    read -r fifo_1 pid_1 < <(spawn $CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT * FROM t_streaming_test STREAM")
    read_until "$fifo_1" "started"

    # insert some data into stream
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test (*) select number, number from numbers(1)"
    read_until "$fifo_1" "0"

    echo "== Alter table: add column c =="

    $CLICKHOUSE_CLIENT "${opts[@]}" -q "ALTER TABLE t_streaming_test ADD COLUMN c UInt64 after b"

    # insert some data into stream
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test (*) select number, number, number from numbers(2)"
    read_until "$fifo_1" "1"

    echo "== Alter table: drop column c =="

    $CLICKHOUSE_CLIENT "${opts[@]}" -q "ALTER TABLE t_streaming_test DROP COLUMN c"

    # insert some data into stream
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test (*) select number, number from numbers(1)"
    read_until "$fifo_1" "0"

    echo "== Alter table: change type b =="

    $CLICKHOUSE_CLIENT "${opts[@]}" -q "ALTER TABLE t_streaming_test MODIFY COLUMN b UInt32"

    # insert some data into stream
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test (*) select number, number from numbers(2)"
    read_until "$fifo_1" "1"

    echo "== Alter table: drop column b =="

    $CLICKHOUSE_CLIENT "${opts[@]}" -q "ALTER TABLE t_streaming_test DROP COLUMN b"

    # open fifo for reading to extend it's lifetime
    # shellcheck disable=2034
    exec {fd}<>$fifo_1

    # insert some data into stream
    $CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test (a) select number from numbers(1)"
    read_until "$fifo_1" "COLUMN" | grep -o 'COLUMN'
}

for engine_type in 'MergeTree ORDER BY a SETTINGS queue_mode=1' 'Memory';
do
   run_test "$engine_type"
done
