#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh
cd "$CURDIR" || exit 1

# Regression for MemoryTracker drift.
#
# To make this test indepedendent from the:
# - MemoryTracking syncing with rss (via AsynchronousMetrics.cpp)
# - and various other allocations in parallel
# Instead of measure diff of the MemoryTracking before beginning and after the
# end of all iterations, it measures MemoryTracking after each executed query
# and see how much time it had been changed.
#
# TODO: Once #15236 will be merged, add it to the "parallel" group in skip_list.json

query="SELECT groupArray(repeat('a', 1000)) FROM numbers(10000) GROUP BY number%10 FORMAT JSON"

function get_MemoryTracking()
{
    $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL&query_profiler_real_time_period_ns=0&query_profiler_cpu_time_period_ns=0&log_queries=0" -d@- <<<"SELECT value FROM system.metrics WHERE metric = 'MemoryTracking'"
}

function test_execute_http()
{
    local i=$1 && shift
    for _ in $(seq 1 "$i"); do
        $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL&max_threads=1&query_profiler_real_time_period_ns=0&query_profiler_cpu_time_period_ns=0&log_queries=0" -d@- <<<"$query" | grep -F DB::Exception:
        get_MemoryTracking >> 01540_MemoryTracking.memory.log
    done
}
function test_execute_tcp()
{
    # NOTE: slow in debug
    local i=$1 && shift
    for _ in $(seq 1 "$i"); do
        ${CLICKHOUSE_CLIENT} --max_threads=1 --query_profiler_real_time_period_ns=0 --query_profiler_cpu_time_period_ns=0 --log_queries=0 -q "$query" >/dev/null
        get_MemoryTracking >> 01540_MemoryTracking.memory.log
    done
}
function test_execute_tcp_one_session()
{
    local i=$1 && shift
    for _ in $(seq 1 "$i"); do
        echo "$query;"
        echo "SELECT metric, value FROM system.metrics WHERE metric = 'MemoryTracking';"
    done | ${CLICKHOUSE_CLIENT} -nm --max_threads=1 --query_profiler_real_time_period_ns=0 --query_profiler_cpu_time_period_ns=0 --log_queries=0 | {
        grep ^MemoryTracking | cut -f2 > 01540_MemoryTracking.memory.log
    }
}

# run test and check the MemoryTracking
function run_test()
{
    local iterations=$1 && shift
    local test_func=$1 && shift

    # 3 changes to MemoryTracking is minimum, since:
    # - this is not that high to not detect inacuracy
    # - memory can go like X/X+N due to some background allocations
    # - memory can go like X/X+N/X, so at least 2 changes
    local changes_allowed=3
    local changes_allowed_auto=$((iterations/6))
    if [[ $changes_allowed_auto -gt $changes_allowed ]]; then
        # if iterations is large enough, use 6% from them
        changes_allowed=$changes_allowed_auto
    fi

    rm -f 01540_MemoryTracking.memory.log
    $test_func "$iterations"

    local changed
    changed="$(awk '{changed+=(prev && prev!=$0); prev=$0; } END {print changed}' 01540_MemoryTracking.memory.log)"

    if [[ $changed -gt $changes_allowed ]]; then
        echo "Test $test_func failed. MemoryTracking changed too frequently: $changed (allowed $changes_allowed)" >&2
    fi
}

# cleanup
trap 'rm -f 01540_MemoryTracking.memory.log' EXIT

echo 'HTTP'
run_test 100 test_execute_http
echo 'TCP_ONE_SESSION'
run_test 100 test_execute_tcp_one_session
echo 'TCP'
run_test 100 test_execute_tcp
echo 'OK'

exit 0
