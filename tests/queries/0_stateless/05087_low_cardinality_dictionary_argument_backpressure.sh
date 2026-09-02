#!/usr/bin/env bash
# Tags: no-parallel
# The owner pause and reduced watermark affect all dictionary-shard queries in this server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_NAME="low_cardinality_dictionary_argument_backpressure"
TEST_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/dictionary_argument_backpressure.XXXXXX")
LOW_THRESHOLD="dictionary_aggregation_argument_backpressure_threshold"
OWNER_PAUSE="dictionary_aggregation_before_drain"
OWNER_EXCEPTION="dictionary_aggregation_throw_before_drain"
QUERY_ID="dictionary_argument_backpressure_${CLICKHOUSE_DATABASE}_$$"
QUERY_PID=""

cleanup()
{
    ${CLICKHOUSE_CLIENT} --ignore-error -q "
        SYSTEM DISABLE FAILPOINT ${OWNER_EXCEPTION};
        SYSTEM DISABLE FAILPOINT ${OWNER_PAUSE};
        SYSTEM DISABLE FAILPOINT ${LOW_THRESHOLD};
        KILL QUERY WHERE query_id = '${QUERY_ID}' ASYNC FORMAT Null;
    " ||:
    if [[ -n "${QUERY_PID}" ]]; then
        timeout 30 tail --pid="${QUERY_PID}" --sleep-interval=0.05 -f /dev/null ||:
        wait "${QUERY_PID}" ||:
    fi
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE_NAME}" ||:
    rm -f "${TEST_DIR}/stdout" "${TEST_DIR}/stderr"
    rmdir "${TEST_DIR}"
}
trap cleanup EXIT

# All dictionaries fit within the writer's default key-count limit. One shared payload
# dictionary is about 256 KiB, although each encoded one-row block contains only an index.
${CLICKHOUSE_CLIENT} -q "
    SET max_threads = 1, max_insert_threads = 1, max_block_size = 64, max_insert_block_size = 64;
    CREATE TABLE ${TABLE_NAME}
    (
        id UInt64,
        key LowCardinality(String),
        value UInt64,
        payload LowCardinality(String),
        small_payload LowCardinality(String)
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 1, index_granularity_bytes = 0,
        min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, min_level_for_wide_part = 0;
    SYSTEM STOP MERGES ${TABLE_NAME};
    INSERT INTO ${TABLE_NAME}
        SELECT number * 2, 'shared', number * 2 + 1,
            concat(leftPad(toString(number), 2, '0'), repeat('x', 8190)),
            concat(leftPad(toString(number), 2, '0'), repeat('x', 254)) FROM numbers(32);
    INSERT INTO ${TABLE_NAME}
        SELECT number * 2 + 1, 'shared', number * 2 + 2,
            concat(leftPad(toString(number), 2, '0'), repeat('y', 8190)),
            concat(leftPad(toString(number), 2, '0'), repeat('y', 254)) FROM numbers(32);
    SELECT throwIf(count() != 2 OR countIf(part_type = 'Wide' AND rows = 32) != 2,
        'Expected two Wide parts with interleaved rows')
    FROM system.parts WHERE database = currentDatabase() AND table = '${TABLE_NAME}' AND active FORMAT Null;
    SYSTEM ENABLE FAILPOINT ${LOW_THRESHOLD};
"

aggregation_query()
{
    local mode="$1" payload_column="payload" aggregate="length(max(payload))"
    if [[ "${mode}" == nested ]]; then
        aggregate="length(tupleElement(max(tuple(payload)), 1))"
    elif [[ "${mode}" == duplicates ]]; then
        payload_column="small_payload"
        local elements="small_payload"
        for ((i = 1; i < 16; ++i)); do elements+=", small_payload"; done
        aggregate="length(tupleElement(max(tuple(${elements})), 1))"
    fi
    local ordered_input="SELECT key, value, ${payload_column} FROM ${TABLE_NAME} ORDER BY id"
    echo "
        SELECT key, count(), sum(value), ${aggregate}
        FROM (${ordered_input} UNION ALL ${ordered_input}) GROUP BY key
        SETTINGS max_threads = 2, max_threads_min_free_memory_per_thread = 0,
            enable_parallel_replicas = 0, max_block_size = 1, preferred_block_size_bytes = 0,
            max_streams_for_union_step = 0, max_streams_for_union_step_to_max_threads_ratio = 0,
            merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0,
            merge_tree_use_deserialization_prefixes_cache = 1,
            optimize_read_in_order = 1, query_plan_remove_redundant_sorting = 0,
            query_plan_lift_up_union = 0, optimize_aggregation_in_order = 0,
            enable_adaptive_aggregator = 0, enable_lazy_columns_replication = 0,
            collect_hash_table_stats_during_aggregation = 0, compile_aggregate_expressions = 0,
            max_rows_to_group_by = 0, group_by_two_level_threshold = 0,
            group_by_two_level_threshold_bytes = 0, max_bytes_before_external_group_by = 0,
            max_bytes_ratio_before_external_group_by = 0,
            max_execution_time = 30, timeout_overflow_mode = 'throw', log_profile_events = 1;
    "
}

wait_for_event()
{
    local event="$1" minimum="${2:-1}" before_poll="${3:-}" observed
    for ((attempt = 0; attempt < 100; ++attempt)); do
        # Run the preceding control command once, together with the first poll.
        # `CLICKHOUSE_CLIENT` contains the executable and its options.
        # shellcheck disable=SC2086
        observed=$(timeout 30 ${CLICKHOUSE_CLIENT} -q "
            ${before_poll}
            SELECT countIf(ProfileEvents['${event}'] >= ${minimum}) > 0
            FROM system.processes WHERE query_id = '${QUERY_ID}';
        ")
        before_poll=""
        if [[ "${observed}" == 1 ]]; then return; fi
        sleep 0.05
    done
    echo "Query did not reach ${event} >= ${minimum} while the owner was paused" >&2
    cat "${TEST_DIR}/stderr" >&2
    return 1
}

start_paused_query()
{
    local mode="$1" event="${2:-AggregationDictionaryBackpressureWaits}" minimum="${3:-1}"
    QUERY_ID="dictionary_argument_backpressure_${CLICKHOUSE_DATABASE}_$$_${mode}"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${OWNER_EXCEPTION}; SYSTEM ENABLE FAILPOINT ${OWNER_PAUSE}"
    ${CLICKHOUSE_CLIENT} --query_id="${QUERY_ID}" -q "$(aggregation_query "${mode}")" >"${TEST_DIR}/stdout" 2>"${TEST_DIR}/stderr" &
    QUERY_PID=$!
    wait_for_event "${event}" "${minimum}" "SYSTEM WAIT FAILPOINT ${OWNER_PAUSE} PAUSE;"
}

finish_query()
{
    local expected_code="${1:-0}" before_release="${2:-}" status=0
    ${CLICKHOUSE_CLIENT} -q "${before_release} SYSTEM DISABLE FAILPOINT ${OWNER_PAUSE};"
    timeout 30 tail --pid="${QUERY_PID}" --sleep-interval=0.05 -f /dev/null
    wait "${QUERY_PID}" || status=$?
    QUERY_PID=""
    if [[ "${expected_code}" == 0 ]]; then
        if [[ "${status}" != 0 ]]; then cat "${TEST_DIR}/stderr" >&2; return 1; fi
        cat "${TEST_DIR}/stdout"
    else
        if [[ "${status}" == 0 ]] || ! grep -Fq "Code: ${expected_code}." "${TEST_DIR}/stderr"; then
            cat "${TEST_DIR}/stderr" >&2
            echo "Expected query failure ${expected_code}, got client status ${status}" >&2
            return 1
        fi
        echo "query failed with ${expected_code}"
    fi
}

for mode in direct nested; do
    echo "${mode} argument"
    # The complete encoded input is smaller than the watermark. Only the retained dictionary
    # can make a producer wait while another owner holds a single one-row block.
    start_paused_query "${mode}"
    echo "retained dictionary applied backpressure"
    finish_query
done

echo "cancellation"
start_paused_query cancel
# Cancellation must wake the producer before the owner is released.
wait_for_event AggregationDictionaryBackpressureCancelledWaits 1 "KILL QUERY WHERE query_id = '${QUERY_ID}' ASYNC FORMAT Null;"
finish_query 394

echo "owner exception"
start_paused_query exception
finish_query 710 "SYSTEM ENABLE FAILPOINT ${OWNER_EXCEPTION};"

echo "duplicate dictionary references"
# Repeated dictionary switches with an owner paused prove the other producer can proceed.
# Counting the same roughly 8 KiB dictionary
# sixteen times for the tuple would exceed the 64 KiB watermark and block that producer.
start_paused_query duplicates AggregationSingleLowCardinalityDictionarySwitches 16
finish_query 0 "
    SELECT throwIf(ProfileEvents['AggregationDictionaryBackpressureWaits'] != 0,
        'Duplicate dictionary references exceeded the watermark')
    FROM system.processes WHERE query_id = '${QUERY_ID}' FORMAT Null;
    SELECT 'duplicate references stayed below the watermark';
"
