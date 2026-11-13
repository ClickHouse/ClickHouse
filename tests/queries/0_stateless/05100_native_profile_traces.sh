#!/usr/bin/env bash
# Tags: no-parallel
# The pre-schema `INSERT` samples every allocation and can exhaust the shared bounded profiler pipe.

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
    local presence="$2"
    ${CLICKHOUSE_LOCAL} --input-format JSONEachRow --structure "$trace_structure" --query "
        SELECT throwIf(countIf(
                (notEmpty('$expected_id') AND query_id != '$expected_id')
                OR empty(host_name) OR empty(query_id)
                OR if(trace_type IN ('Dropped', 'Incomplete'),
                    thread_id != 0 OR event_time_microseconds != 0 OR notEmpty(trace) OR notEmpty(symbols)
                        OR if(trace_type = 'Dropped', size <= 0, size != 0),
                    thread_id = 0 OR event_time_microseconds = 0 OR empty(trace) OR length(trace) != length(symbols)
                        OR trace_type NOT IN ('CPU', 'Real', 'Memory', 'MemorySample', 'MemoryPeak'))) > 0,
                'Invalid streamed profile trace')
            + toUInt8($presence)
        FROM table"
}

observe_samples()
{
    local expected_id="$1"
    local presence="$2"
    shift 2
    local sample_log="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_profile_traces_observation_${RANDOM}.jsonl"
    # Short queries can lose all their samples in the shared, bounded profiler pipe.
    for attempt in 1 2 3 4; do
        ${CLICKHOUSE_CLIENT} "$@" > "$sample_log" 2>&1
        local observed
        observed=$(check_samples "$expected_id" "$presence" < "$sample_log")
        if [[ "$observed" == 1 ]]; then
            echo 1
            return
        fi
        if [[ "$attempt" != 4 ]]; then sleep 0.1; fi
    done
    echo 0
}

memory_presence="countIf(trace_type = 'MemorySample' AND size != 0) > 0"
for compression in true false; do
    echo "compression=$compression, full query ID"
    query_id="${query_prefix}_${compression}"
    observe_samples "$query_id" "$memory_presence" "${profile_options[@]}" --send_profile_traces=1 --compression="$compression" --query_id="$query_id" \
        --query "$sample_query FORMAT Null"
done

echo 'traces before the INSERT schema preserve the server exception'
insert_error_log="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_profile_traces_insert_error_${RANDOM}.log"
for attempt in 1 2 3 4; do
    insert_error_code=0
    ${CLICKHOUSE_CLIENT} --print-profile-traces --send_profile_traces=1 --send_profile_events=0 --send_logs_level=none \
        --query_profiler_cpu_time_period_ns=0 --query_profiler_real_time_period_ns=0 --memory_profiler_step=0 \
        --memory_profiler_sample_probability=1 --memory_profiler_sample_min_allocation_size=0 --max_untracked_memory=0 \
        --query "INSERT INTO ${CLICKHOUSE_DATABASE}.profile_traces_missing_${RANDOM} FORMAT Values" \
        <<< '(1)' > "$insert_error_log" 2>&1 || insert_error_code=$?
    test "$insert_error_code" -eq 60
    grep -q '(UNKNOWN_TABLE)' "$insert_error_log"
    if grep -q '"trace_type":"MemorySample"' "$insert_error_log"; then break; fi
    if [[ "$attempt" != 4 ]]; then sleep 0.1; fi
done
grep -q '"trace_type":"MemorySample"' "$insert_error_log"
echo 1

echo 'SQL setting enables trace packets'
observe_samples "${query_prefix}_sql" "$memory_presence" "${profile_options[@]}" --send_profile_traces=0 --query_id="${query_prefix}_sql" \
    --query "$sample_query FORMAT Null SETTINGS send_profile_traces=1"

echo 'disabled delivery emits no traces'
${CLICKHOUSE_CLIENT} "${profile_options[@]}" --send_profile_traces=0 --query "$sample_query FORMAT Null" 2>&1 \
    | ${CLICKHOUSE_LOCAL} --input-format LineAsString --query 'SELECT count() = 0 FROM table'

echo 'SQL setting disables trace packets'
${CLICKHOUSE_CLIENT} "${profile_options[@]}" --send_profile_traces=1 --query "$sample_query FORMAT Null SETTINGS send_profile_traces=0" 2>&1 \
    | ${CLICKHOUSE_LOCAL} --input-format LineAsString --query 'SELECT count() = 0 FROM table'

echo 'connection reuse resets packet readers and query subscriptions'
observe_samples "" "throwIf(uniqExact(query_id) > 2, 'Disabled query emitted traces') + toUInt8(uniqExact(query_id) = 2)" \
    "${profile_options[@]}" --send_profile_traces=1 --query "
    $sample_query FORMAT Null;
    $sample_query FORMAT Null SETTINGS send_profile_traces=0;
    $sample_query FORMAT Null"

for hedged in 0 1; do
    echo "remote traces, use_hedged_requests=$hedged"
    query_id="${query_prefix}_remote_${hedged}"
    observe_samples "" "countIf(query_id != '$query_id') > 0" "${profile_options[@]}" --send_profile_traces=1 \
        --prefer_localhost_replica=0 --use_hedged_requests="$hedged" --query_id="$query_id" --query "
        SELECT * FROM remote('$CLICKHOUSE_HOST:$CLICKHOUSE_PORT_TCP', view($sample_query)) FORMAT Null"
done
