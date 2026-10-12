#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: the test pauses the server-wide trace collector.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT trace_collector_pause_before_trace"' EXIT

# Traces are dropped once the paused collector's pipe is full, so only the few allocations of at least 1 MiB are sampled.
sampled=(--memory_profiler_sample_probability=1 --max_untracked_memory=0 --memory_profiler_sample_min_allocation_size=1048576)
query="SELECT groupArray(number) FROM numbers(1000000) FORMAT Null"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT trace_collector_pause_before_trace"
# Any trace makes the collector reach the failpoint; this query guarantees one.
${CLICKHOUSE_CLIENT} "${sampled[@]}" -q "$query"
timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT trace_collector_pause_before_trace PAUSE"

query_id="${CLICKHOUSE_DATABASE}_sampled"
${CLICKHOUSE_CLIENT} --query_id="$query_id" "${sampled[@]}" -q "$query"

flush_id="${CLICKHOUSE_DATABASE}_flush"
${CLICKHOUSE_CLIENT} --query_id="$flush_id" -q "SYSTEM FLUSH LOGS trace_log" &
flush_pid=$!
# Resume the collector only after the flush has started.
while kill -0 "$flush_pid" 2>/dev/null \
    && [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '$flush_id'")" = 0 ]; do
    sleep 0.1
done
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT trace_collector_pause_before_trace"
wait "$flush_pid"

${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM system.trace_log WHERE event_date >= yesterday()
    AND event_time >= now() - 600 AND query_id = '$query_id' AND trace_type = 'MemorySample'"
