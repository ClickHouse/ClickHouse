#!/usr/bin/env bash
# Tags: long, no-parallel, no-tsan, no-asan, no-debug, no-s3-storage, no-fasttest, no-replicated-database

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function thread1()
{
    thread_id=$1
    while true; do
        query_id="02497_$CLICKHOUSE_DATABASE-$RANDOM-$thread_id"
        $CLICKHOUSE_CLIENT --query_id=$query_id --query "
            SELECT count() FROM numbers_mt(100000) SETTINGS
                trace_profile_events = 1,
                query_profiler_real_time_period_ns = 10000000,
                query_profiler_cpu_time_period_ns =  10000000,
                memory_profiler_step = 1024,
                memory_profiler_sample_probability = 0.9
            "
    done
}

function thread2()
{
    while true; do
        $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
    done
}

export -f thread1
export -f thread2

TIMEOUT=10

timeout $TIMEOUT bash -c "thread1 0" >/dev/null &
timeout $TIMEOUT bash -c "thread1 1" >/dev/null &
timeout $TIMEOUT bash -c "thread1 2" >/dev/null &
timeout $TIMEOUT bash -c "thread1 3" >/dev/null &
timeout $TIMEOUT bash -c thread2 >/dev/null &

wait

for _ in {1..10}
do
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id LIKE '02497_$CLICKHOUSE_DATABASE%' SYNC" >/dev/null

    # After this moment, the server can still run another query.
    # For example, the 'timeout' command killed all threads of thread1,
    # and the 'timeout' itself has finished, and we have successfully 'wait'-ed for it,
    # but just before that, one of the threads successfully sent a query to the server,
    # but the server didn't start to run this query yet,
    # and even when the KILL QUERY was run, the query from the thread didn't start,
    # but only started after the KILL QUERY has been already processed.

    # That's why we have to run this in a loop.

    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id LIKE '02497_$CLICKHOUSE_DATABASE%'" | rg '^0$' && break

    sleep 1
done
