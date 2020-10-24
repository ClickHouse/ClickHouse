#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# Regression for MemoryTracker drift via HTTP queries.
#
# For this will be used:
# - max_memory_usage_for_user
# - one users' query in background (to avoid reseting max_memory_usage_for_user)

query="SELECT groupArray(repeat('a', 1000)) FROM numbers(10000) GROUP BY number%10 FORMAT JSON"

function execute_http()
{
    for _ in {1..100}; do
        $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL&max_memory_usage_for_user=100Mi&max_threads=1" -d@- <<<"$query" | grep -F DB::Exception:
    done
}
function execute_tcp()
{
    # slow in debug, but should trigger the problem in ~10 iterations, so 20 is ok
    for _ in {1..20}; do
        ${CLICKHOUSE_CLIENT} --max_memory_usage_for_user=100Mi --max_threads=1 -q "$query" | grep -F DB::Exception:
    done
}
function execute_tcp_one_session()
{
    for _ in {1..30}; do
        echo "$query;"
    done | ${CLICKHOUSE_CLIENT} -nm --max_memory_usage_for_user=100Mi --max_threads=1 | grep -F DB::Exception:
}


# one users query in background (to avoid reseting max_memory_usage_for_user)
# --max_block_size=1 to make it killable (check the state each 1 second, 1 row)
# (the test takes ~40 seconds in debug build, so 60 seconds is ok)
${CLICKHOUSE_CLIENT} --max_block_size=1 --format Null -q 'SELECT sleepEachRow(1) FROM numbers(600)' &
# trap
sleep_query_pid=$!
function cleanup()
{
    echo 'KILL sleep'
    # if the timeout will not be enough, it will trigger "No such process" error/message
    kill $sleep_query_pid
}
trap cleanup EXIT

echo 'HTTP'
execute_http
echo 'TCP_ONE_SESSION'
execute_tcp_one_session
echo 'TCP'
execute_tcp
echo 'OK'

exit 0
