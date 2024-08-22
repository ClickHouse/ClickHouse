#!/usr/bin/env bash
# Tags: long, no-parallel, no-tsan, no-asan, no-debug, no-object-storage, no-fasttest, no-replicated-database

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function thread1()
{
    thread_id=$1
    local TIMELIMIT=$((SECONDS+$2))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
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
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
    done
}

export -f thread1
export -f thread2

TIMEOUT=10

thread1 0 $TIMEOUT >/dev/null &
thread1 1 $TIMEOUT >/dev/null &
thread1 2 $TIMEOUT >/dev/null &
thread1 3 $TIMEOUT >/dev/null &
thread2 $TIMEOUT >/dev/null &

wait

for _ in {1..10}
do
    # process list is cleaned after everything is sent to client
    # so this check can be run before process list is cleaned
    # to avoid spurious failures we retry the check couple of times
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id LIKE '02497_$CLICKHOUSE_DATABASE%'" | rg '^0$' && break
    sleep 1
done