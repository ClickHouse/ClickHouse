#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-ubsan, no-msan, no-cpu-aarch64
# requires TraceCollector, does not available under sanitizers and aarch64

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query="select randomPrintableASCII(number) from numbers(1000)"
# at least 2, one allocation, one deallocation
# (but actually even more)
min_trace_entries=2

# TCP

# do not use _, they should be escaped for LIKE
query_id_tcp_prefix="01526-tcp-memory-tracking-$RANDOM-$$"
${CLICKHOUSE_CLIENT} --log_queries=1 --max_threads=1 --max_untracked_memory=0 --memory_profiler_sample_probability=1 -q "with '$query_id_tcp_prefix' as __id $query FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"
query_id_tcp="$(${CLICKHOUSE_CLIENT} -q "SELECT DISTINCT query_id FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%$query_id_tcp_prefix%'")"
${CLICKHOUSE_CLIENT} -q "SELECT count()>=$min_trace_entries FROM system.trace_log WHERE query_id = '$query_id_tcp' AND abs(size) < 4e6 AND event_time >= now() - interval 1 hour"

# HTTP

# query_id cannot be longer then 28 bytes
query_id_http="01526_http_${RANDOM}_$$"
echo "$query" | ${CLICKHOUSE_CURL} -sSg -o /dev/null "${CLICKHOUSE_URL}&query_id=$query_id_http&max_untracked_memory=0&memory_profiler_sample_probability=1&max_threads=1" -d @-
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"
# at least 2, one allocation, one deallocation
# (but actually even more)
${CLICKHOUSE_CLIENT} -q "SELECT count()>=$min_trace_entries FROM system.trace_log WHERE query_id = '$query_id_http' AND abs(size) < 4e6 AND event_time >= now() - interval 1 hour"
