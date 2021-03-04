#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# Regression for MemoryTracker drift via HTTP queries.
#
# For this will be used:
# - max_memory_usage_for_user
# - one users' query in background (to avoid reseting max_memory_usage_for_user)

# We have to create a separate user to run this tests isolated from other tests.
${CLICKHOUSE_CLIENT} -n --allow_introspection_functions 1 --query "DROP USER IF EXISTS test_01541; CREATE USER test_01541; GRANT ALL ON *.* TO test_01541;";


query="SELECT groupArray(repeat('a', 1000)) FROM numbers(10000) GROUP BY number % 10 FORMAT JSON"

function execute_http()
{
    for _ in {1..100}; do
        $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL&user=test_01541&max_memory_usage_for_user=100Mi&max_threads=1" -d@- <<<"$query" | grep -F DB::Exception:
    done
}
function execute_tcp()
{
    # slow in debug, but should trigger the problem in ~10 iterations, so 20 is ok
    for _ in {1..20}; do
        ${CLICKHOUSE_CLIENT} --user=test_01541 --max_memory_usage_for_user=100Mi --max_threads=1 -q "$query" | grep -F DB::Exception:
    done
}
function execute_tcp_one_session()
{
    for _ in {1..30}; do
        echo "$query;"
    done | ${CLICKHOUSE_CLIENT} --user=test_01541 -nm --max_memory_usage_for_user=100Mi --max_threads=1 | grep -F DB::Exception:
}


# one users query in background (to avoid reseting max_memory_usage_for_user)
# --max_block_size=1 to make it killable (check the state each 1 second, 1 row)
# (the test takes ~40 seconds in debug build, so 60 seconds is ok)
query_id=$$-$RANDOM-$SECONDS
${CLICKHOUSE_CLIENT} --user=test_01541 --max_block_size=1 --format Null --query_id $query_id -q 'SELECT sleepEachRow(1) FROM numbers(600)' &
# trap
sleep_query_pid=$!
function cleanup()
{
    echo 'KILL sleep'
    # if the timeout will not be enough, it will trigger "No such process" error/message
    kill $sleep_query_pid
    # waiting for a query to finish
    while ${CLICKHOUSE_CLIENT} -q "SELECT query_id FROM system.processes WHERE query_id = '$query_id'" | grep -xq "$query_id"; do
        sleep 0.1
    done
}
trap cleanup EXIT

echo 'HTTP'
execute_http
echo 'TCP_ONE_SESSION'
execute_tcp_one_session
echo 'TCP'
execute_tcp
echo 'OK'

${CLICKHOUSE_CLIENT} --query "DROP USER test_01541";

exit 0
