#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-msan, no-ubsan, no-fasttest, no-debug
#       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# NOTE: jemalloc is disabled under sanitizers

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id="03594_jemalloc_per_query_sampling_$CLICKHOUSE_TEST_UNIQUE_NAME"
${CLICKHOUSE_CLIENT} --jemalloc_enable_profiler=1 --jemalloc_collect_profile_samples_in_trace_log=1 --query_id="$query_id" -q "SELECT number FROM numbers(1000000) ORDER BY number FORMAT Null";

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS trace_log";

${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM system.trace_log WHERE query_id = '$query_id' AND trace_type = 'JemallocSample'";
