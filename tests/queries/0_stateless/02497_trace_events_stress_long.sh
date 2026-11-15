#!/usr/bin/env bash
# Tags: long, no-parallel, no-tsan, no-asan, no-debug, no-object-storage, no-fasttest, no-replicated-database, no-flaky-check

# `no-flaky-check`: this test floods `system.trace_log` on purpose (four threads at a 10 ms profiler
# period with `trace_profile_events` and a 0.9 memory profiler sample probability) while a fifth thread
# runs `SYSTEM FLUSH LOGS trace_log` in a tight loop. The flaky check repeats it back to back a few
# dozen times, and any error printed by a client fails the test, so a single flush eventually exceeds
# the 180 s `waitFlush` deadline and reports `TIMEOUT_EXCEEDED`. In a normal run, where one copy runs
# among diverse tests, the test is stable, so only the flaky check is excluded.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The sampling query profiler is not supported under MemorySanitizer, where a non-zero
# `query_profiler_real_time_period_ns` / `query_profiler_cpu_time_period_ns` is rejected with
# `NOT_IMPLEMENTED`. Everything else this test stresses - `trace_profile_events` and the memory
# profiler, both of which feed `TraceCollector` from ordinary code - stays supported there, so drop
# only these two settings on that build instead of skipping the test.
QUERY_PROFILER_SETTINGS="query_profiler_real_time_period_ns = 10000000, query_profiler_cpu_time_period_ns = 10000000,"
if [ "$($CLICKHOUSE_LOCAL -q "SELECT value LIKE '%-fsanitize=memory%' FROM system.build_options WHERE name = 'CXX_FLAGS'")" = "1" ]; then
    QUERY_PROFILER_SETTINGS=""
fi
export QUERY_PROFILER_SETTINGS

function thread1()
{
    thread_id=$1
    local TIMELIMIT=$((SECONDS+$2))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        query_id="02497_$CLICKHOUSE_DATABASE-$RANDOM-$thread_id"
        $CLICKHOUSE_CLIENT --query_id=$query_id --query "
            SELECT count() FROM numbers_mt(100000) SETTINGS
                trace_profile_events = 1,
                $QUERY_PROFILER_SETTINGS
                memory_profiler_step = 1024,
                memory_profiler_sample_probability = 0.9
            "
    done
}

function thread2()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS trace_log"
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
