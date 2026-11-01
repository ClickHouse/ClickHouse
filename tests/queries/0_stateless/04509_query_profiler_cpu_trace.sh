#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-msan, no-ubsan, no-sanitize-coverage, no-llvm-coverage

# Check that the CPU query profiler emits trace_type='CPU' rows (a separate sampler-thread path on macOS).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="04509_cpu_profiler_${CLICKHOUSE_DATABASE}"

# CPU-bound query bounded to ~1s (break, not throw); settings pinned so randomized ones can't slow it.
${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "
    SELECT count() FROM numbers(1000000000000)
    SETTINGS query_profiler_real_time_period_ns = 0,
             query_profiler_cpu_time_period_ns = 1000000,
             trace_profile_events = 0,
             max_execution_time = 1,
             timeout_overflow_mode = 'break',
             max_threads = 1,
             max_block_size = 65505,
             max_rows_to_read = 0
    FORMAT Null"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS trace_log"

${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0
    FROM system.trace_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND query_id = '$query_id' AND trace_type = 'CPU'"
