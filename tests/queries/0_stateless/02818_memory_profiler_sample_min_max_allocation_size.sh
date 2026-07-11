#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-ubsan, no-msan, no-random-settings, no-llvm-coverage
# no-llvm-coverage: LLVM source-based coverage instrumentation makes the server ~3-5x slower,
# which causes `SystemLogQueue<TraceLogElement>` flushes to exceed the 180s server-side deadline
# when the test emits `MemorySample` trace rows with `memory_profiler_sample_probability = 1`.
# The resulting stderr `Code: 159 TIMEOUT_EXCEEDED` contaminates the reference output.
# Same pattern as sibling coverage fixes (#103287, #103288, #103293, #103298, #103226, #103219).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

trace_settings=(
  --memory_profiler_sample_min_allocation_size=4096
  --memory_profiler_sample_max_allocation_size=16384
  --log_queries=1
  --max_threads=1
  --memory_profiler_sample_probability=1
  --query "select randomPrintableASCII(number) from numbers(1000) FORMAT Null"
)

# Flush path: every allocation exceeds max_untracked_memory and goes through MemoryTracker::allocImpl directly.
query_id_flush="${CLICKHOUSE_DATABASE}_min_max_allocation_size_flush"
${CLICKHOUSE_CLIENT} "${trace_settings[@]}" --query_id="$query_id_flush" --max_untracked_memory=0

# Non-flush path: small allocations stay below max_untracked_memory and sample via the cached ThreadStatus::sample_* values,
# which must be refreshed after ProcessList::insert applies query-level memory_profiler_sample_* settings.
query_id_cached="${CLICKHOUSE_DATABASE}_min_max_allocation_size_cached"
${CLICKHOUSE_CLIENT} "${trace_settings[@]}" --query_id="$query_id_cached" --max_untracked_memory=4Mi

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS trace_log"

${CLICKHOUSE_CLIENT} --query "
SELECT
  countIf(query_id = '$query_id_flush') > 0 AS flush_has_samples,
  countIf(query_id = '$query_id_cached') > 0 AS cached_has_samples,
  countIf(abs(size) > 16384 OR abs(size) < 4096) AS out_of_range
FROM system.trace_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND query_id IN ('$query_id_flush', '$query_id_cached')
  AND trace_type = 'MemorySample'
FORMAT Vertical
"
