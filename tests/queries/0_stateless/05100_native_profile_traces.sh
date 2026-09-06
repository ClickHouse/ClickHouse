#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

profile_options=(
    --print-profile-traces --send_profile_events=0 --send_logs_level=none
    --query_profiler_cpu_time_period_ns=0 --query_profiler_real_time_period_ns=0
    --memory_profiler_sample_probability=1 --memory_profiler_sample_min_allocation_size=65536
    --max_untracked_memory=0
)
trace_structure='host_name String, query_id String, trace_type String, thread_id UInt64, event_time_microseconds UInt64, trace Array(UInt64), symbols Array(String), size Int64'
sample_query='SELECT sum(cityHash64(toString(number))) FROM numbers(200000)'
query_prefix="${CLICKHOUSE_DATABASE}_native_profile_traces_$(printf '%0120d' 0)"

check_samples()
{
    local expected_id="$1"
    ${CLICKHOUSE_LOCAL} --input-format JSONEachRow --structure "$trace_structure" --query "
        SELECT count() > 0
            AND countIf(query_id != '$expected_id') = 0
            AND countIf(empty(host_name) OR thread_id = 0 OR event_time_microseconds = 0) = 0
            AND countIf(empty(trace) OR length(trace) != length(symbols)) = 0
            AND countIf(trace_type = 'MemorySample' AND size != 0) > 0
        FROM table"
}

for compression in true false; do
    echo "compression=$compression, full query ID"
    query_id="${query_prefix}_${compression}"
    ${CLICKHOUSE_CLIENT} "${profile_options[@]}" --send_profile_traces=1 --compression="$compression" --query_id="$query_id" \
        --query "$sample_query FORMAT Null" 2>&1 | check_samples "$query_id"
done

echo 'traces before the INSERT schema preserve the server exception'
insert_error_log="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_profile_traces_insert_error_${RANDOM}.log"
insert_error_code=0
${CLICKHOUSE_CLIENT} --print-profile-traces --send_profile_traces=1 --send_profile_events=0 --send_logs_level=none \
    --query_profiler_cpu_time_period_ns=0 --query_profiler_real_time_period_ns=0 --memory_profiler_step=0 \
    --memory_profiler_sample_probability=1 --memory_profiler_sample_min_allocation_size=0 --max_untracked_memory=0 \
    --query "INSERT INTO ${CLICKHOUSE_DATABASE}.profile_traces_missing_${RANDOM} FORMAT Values" \
    <<< '(1)' > "$insert_error_log" 2>&1 || insert_error_code=$?
test "$insert_error_code" -eq 60
grep -q '"trace_type":"MemorySample"' "$insert_error_log"
grep -q '(UNKNOWN_TABLE)' "$insert_error_log"
echo 1

echo 'SQL setting enables trace packets'
${CLICKHOUSE_CLIENT} "${profile_options[@]}" --send_profile_traces=0 --query_id="${query_prefix}_sql" \
    --query "$sample_query FORMAT Null SETTINGS send_profile_traces=1" 2>&1 | check_samples "${query_prefix}_sql"

echo 'disabled delivery emits no traces'
${CLICKHOUSE_CLIENT} "${profile_options[@]}" --send_profile_traces=0 --query "$sample_query FORMAT Null" 2>&1 \
    | ${CLICKHOUSE_LOCAL} --input-format LineAsString --query 'SELECT count() = 0 FROM table'

echo 'SQL setting disables trace packets'
${CLICKHOUSE_CLIENT} "${profile_options[@]}" --send_profile_traces=1 --query "$sample_query FORMAT Null SETTINGS send_profile_traces=0" 2>&1 \
    | ${CLICKHOUSE_LOCAL} --input-format LineAsString --query 'SELECT count() = 0 FROM table'

echo 'connection reuse resets packet readers and query subscriptions'
${CLICKHOUSE_CLIENT} "${profile_options[@]}" --send_profile_traces=1 --query "
    $sample_query FORMAT Null;
    $sample_query FORMAT Null SETTINGS send_profile_traces=0;
    $sample_query FORMAT Null" 2>&1 \
    | ${CLICKHOUSE_LOCAL} --input-format JSONEachRow --structure "$trace_structure" \
        --query 'SELECT count() > 0 AND uniqExact(query_id) = 2 FROM table'

for hedged in 0 1; do
    echo "remote traces, use_hedged_requests=$hedged"
    query_id="${query_prefix}_remote_${hedged}"
    ${CLICKHOUSE_CLIENT} "${profile_options[@]}" --send_profile_traces=1 --prefer_localhost_replica=0 --use_hedged_requests="$hedged" --query_id="$query_id" --query "
        SELECT * FROM remote('$CLICKHOUSE_HOST:$CLICKHOUSE_PORT_TCP', view($sample_query)) FORMAT Null" 2>&1 \
        | ${CLICKHOUSE_LOCAL} --input-format JSONEachRow --structure "$trace_structure" --query "
            SELECT countIf(query_id != '$query_id') > 0
                AND countIf(empty(host_name) OR empty(query_id) OR empty(trace) OR length(trace) != length(symbols)) = 0
            FROM table"
done
