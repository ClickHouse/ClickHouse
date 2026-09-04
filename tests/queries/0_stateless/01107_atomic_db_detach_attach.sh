#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

NEW_DATABASE=test_01107_${CLICKHOUSE_DATABASE}

wait_for_insert_to_start()
{
    local query_id=$1

    for _ in $(seq 1 600)
    do
        if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id' AND read_rows > 0")" = 1 ]
        then
            return 0
        fi
        sleep 0.1
    done

    echo "Timed out waiting for INSERT query $query_id to start reading" >&2
    return 1
}

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${NEW_DATABASE}"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${NEW_DATABASE} ENGINE=Atomic"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${NEW_DATABASE}.mt (n UInt64) ENGINE=Log"

first_insert_query_id=01107_first_insert_$CLICKHOUSE_DATABASE
$CLICKHOUSE_CLIENT --query_id "$first_insert_query_id" --function_sleep_max_microseconds_per_block 60000000 -q "INSERT INTO ${NEW_DATABASE}.mt SELECT number + sleepEachRow(3) FROM numbers(5)" &
wait_for_insert_to_start "$first_insert_query_id"

$CLICKHOUSE_CLIENT -q "DETACH TABLE ${NEW_DATABASE}.mt" --database_atomic_wait_for_drop_and_detach_synchronously=0
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${NEW_DATABASE}.mt" --database_atomic_wait_for_drop_and_detach_synchronously=0 2>&1 | grep -F "Code: 57" > /dev/null && echo "OK"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${NEW_DATABASE}" --database_atomic_wait_for_drop_and_detach_synchronously=0 2>&1 | grep -F "Code: 219" > /dev/null && echo "OK"

wait
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${NEW_DATABASE}.mt"
$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM ${NEW_DATABASE}.mt"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${NEW_DATABASE}" --database_atomic_wait_for_drop_and_detach_synchronously=0
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${NEW_DATABASE}"
$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM ${NEW_DATABASE}.mt"

second_insert_query_id=01107_second_insert_$CLICKHOUSE_DATABASE
# `dropped` has to be printed before `end`: the point of the asynchronous `DROP DATABASE` below is
# that it returns while this INSERT is still running instead of waiting for it. The INSERT therefore
# has to still be in flight once the drop returns, so it sleeps as long as the first one - each
# `clickhouse-client` start costs seconds on a loaded machine, and the poll loop needs a few of them
# before it sees the query, which a shorter INSERT can outlive. For the same reason the drop does not
# sleep before announcing itself: it returns immediately, so making the two messages race over a
# fixed one-second window only narrows the margin instead of ordering them.
$CLICKHOUSE_CLIENT --query_id "$second_insert_query_id" --function_sleep_max_microseconds_per_block 60000000 -q "INSERT INTO ${NEW_DATABASE}.mt SELECT number + sleepEachRow(3) FROM numbers(5)" && echo "end" &
wait_for_insert_to_start "$second_insert_query_id"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${NEW_DATABASE}" --database_atomic_wait_for_drop_and_detach_synchronously=0 && echo "dropped"
wait
