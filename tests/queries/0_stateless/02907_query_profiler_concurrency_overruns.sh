#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-fasttest, no-cpu-aarch64

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The check is probablistic, so make sure that it passes at least sometimes:

while true
do
    ${CLICKHOUSE_CLIENT} -n --query="
    SELECT count() FROM numbers_mt(1000000) SETTINGS
        query_profiler_real_time_period_ns = 1000000,
        query_profiler_cpu_time_period_ns = 1000000,
        max_threads = 1000,
        max_block_size = 100;
    SELECT anyIf(value, event = 'QueryProfilerRuns') > 0, anyIf(value, event = 'QueryProfilerConcurrencyOverruns') > 0 FROM system.events;
    " | tr '\t\n' '  ' | grep '1000000000 1 1' && break
    sleep 1
done
