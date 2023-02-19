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

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id LIKE '02497_$CLICKHOUSE_DATABASE%' SYNC" >/dev/null
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id LIKE '02497_$CLICKHOUSE_DATABASE%'"
