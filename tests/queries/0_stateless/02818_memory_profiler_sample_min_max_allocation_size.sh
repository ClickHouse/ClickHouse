#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-ubsan, no-msan, no-random-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id="${CLICKHOUSE_DATABASE}_min_max_allocation_size_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --query_id="$query_id" --memory_profiler_sample_min_allocation_size=4096 --memory_profiler_sample_max_allocation_size=16384 --log_queries=1 --max_threads=1 --max_untracked_memory=0 --memory_profiler_sample_probability=1 --query "select randomPrintableASCII(number) from numbers(1000) FORMAT Null"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"

# at least something allocated
${CLICKHOUSE_CLIENT} --query "SELECT countDistinct(abs(size)) > 0 FROM system.trace_log where query_id='$query_id' and trace_type = 'MemorySample'"

# show wrong allocations
${CLICKHOUSE_CLIENT} --query "SELECT abs(size) FROM system.trace_log where query_id='$query_id' and trace_type = 'MemorySample' and (abs(size) > 16384 or abs(size) < 4096)"
