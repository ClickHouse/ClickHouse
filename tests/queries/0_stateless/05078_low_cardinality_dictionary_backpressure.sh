#!/usr/bin/env bash
# Tags: no-parallel
# The owner pause and injected exception affect all dictionary-shard queries in this server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_NAME="low_cardinality_dictionary_backpressure"
TEST_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/dictionary_backpressure.XXXXXX")
LOW_THRESHOLD="dictionary_aggregation_low_backpressure_threshold"
OWNER_PAUSE="dictionary_aggregation_before_drain"
OWNER_EXCEPTION="dictionary_aggregation_throw_before_drain"
QUERY_ID="dictionary_backpressure_${CLICKHOUSE_DATABASE}_$$"
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

INITIAL_QUERIES="
    SET max_threads = 1, max_insert_threads = 1, max_block_size = 64, max_insert_block_size = 64;
    DROP TABLE IF EXISTS ${TABLE_NAME};
    CREATE TABLE ${TABLE_NAME}
    (
        id UInt64,
        key LowCardinality(String),
        value UInt64,
        payload String
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS
        index_granularity = 1,
        index_granularity_bytes = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0,
        min_level_for_wide_part = 0;
    SYSTEM STOP MERGES ${TABLE_NAME};
    INSERT INTO ${TABLE_NAME} SELECT number * 2, 'shared', number * 2 + 1, repeat('x', 4096) FROM numbers(32);
    INSERT INTO ${TABLE_NAME} SELECT number * 2 + 1, 'shared', number * 2 + 2, repeat('x', 4096) FROM numbers(32);
    SELECT throwIf(count() != 2 OR countIf(part_type = 'Wide' AND rows = 32) != 2,
        'Expected two Wide parts with interleaved rows')
    FROM system.parts
    WHERE database = currentDatabase() AND table = '${TABLE_NAME}' AND active
    FORMAT Null;
    SYSTEM ENABLE FAILPOINT ${LOW_THRESHOLD};
"

# Each sorted input alternates A/B/A/... dictionaries, so it must enter the shared-shard
# path regardless of which read tasks the scheduler assigns. Two UNION ALL inputs provide
# independent producers; the byte budget must be shared even across different dictionaries.
ORDERED_INPUT="SELECT key, value, payload FROM ${TABLE_NAME} ORDER BY id"

aggregation_query()
{
    local inputs="$1" threshold="$2" spill_bytes="$3" threads="$4"
    echo "
        SELECT key, count(), sum(value), length(max(payload))
        FROM (${inputs}) GROUP BY key
        SETTINGS
            max_threads = ${threads},
            max_block_size = 1,
            preferred_block_size_bytes = 0,
            merge_tree_min_rows_for_concurrent_read = 1,
            merge_tree_min_bytes_for_concurrent_read = 0,
            merge_tree_use_deserialization_prefixes_cache = 1,
            optimize_read_in_order = 1,
            query_plan_remove_redundant_sorting = 0,
            optimize_aggregation_in_order = 0,
            enable_adaptive_aggregator = 0,
            collect_hash_table_stats_during_aggregation = 0,
            max_rows_to_group_by = 0,
            group_by_two_level_threshold = ${threshold},
            group_by_two_level_threshold_bytes = 0,
            max_bytes_before_external_group_by = ${spill_bytes},
            max_bytes_ratio_before_external_group_by = 0,
            max_execution_time = 30,
            timeout_overflow_mode = 'throw',
            log_queries = 1,
            log_profile_events = 1,
            log_queries_probability = 1,
            log_queries_min_query_duration_ms = 0;
    "
}

wait_for_event()
{
    local event="$1" before_poll="${2:-}" observed
    for ((attempt = 0; attempt < 100; ++attempt)); do
        # Run the preceding control command once, together with the first poll.
        # `CLICKHOUSE_CLIENT` contains the executable and its options.
        # shellcheck disable=SC2086
        observed=$(timeout 30 ${CLICKHOUSE_CLIENT} -q "
            ${before_poll}
            SELECT countIf(ProfileEvents['${event}'] > 0) > 0
            FROM system.processes WHERE query_id = '${QUERY_ID}';
        ") || return
        before_poll=""
        if [[ "${observed}" == 1 ]]; then
            return
        fi
        sleep 0.05
    done
    echo "Query did not report ${event} while the owner was paused" >&2
    cat "${TEST_DIR}/stderr" >&2
    return 1
}

start_paused_query()
{
    local suffix="$1" threshold="$2" spill_bytes="$3" input="${4:-${ORDERED_INPUT}}"
    QUERY_ID="dictionary_backpressure_${CLICKHOUSE_DATABASE}_$$_${suffix}"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT ${OWNER_PAUSE}"
    ${CLICKHOUSE_CLIENT} --query_id="${QUERY_ID}" -q "$(aggregation_query "${input} UNION ALL ${input}" "${threshold}" "${spill_bytes}" 2)" >"${TEST_DIR}/stdout" 2>"${TEST_DIR}/stderr" &
    QUERY_PID=$!
    # The paused owner already swapped its pending queue into a lease. Counting only the
    # pending vector would let the other producer finish without ever waiting.
    wait_for_event AggregationDictionaryBackpressureWaits "SYSTEM WAIT FAILPOINT ${OWNER_PAUSE} PAUSE;"
    echo "producer waited for owner-held input"
}

finish_query()
{
    local expected_code="$1" status=0
    timeout 30 tail --pid="${QUERY_PID}" --sleep-interval=0.05 -f /dev/null
    wait "${QUERY_PID}" || status=$?
    QUERY_PID=""
    if [[ "${expected_code}" == 0 ]]; then
        if [[ "${status}" != 0 ]]; then
            cat "${TEST_DIR}/stderr" >&2
            return 1
        fi
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

for threshold in 0 1; do
    # With a one-byte watermark every input chunk is oversized. A single producer must
    # still make progress: drain all owned shards before testing the watermark.
    INITIAL_QUERIES+="
        SELECT 'single producer, two-level threshold ${threshold}';
        $(aggregation_query "${ORDERED_INPUT}" "${threshold}" 1073741824 1)
    "
done
${CLICKHOUSE_CLIENT} -q "${INITIAL_QUERIES}"

for threshold in 0 1; do
    echo "multiple producers, two-level threshold ${threshold}"
    start_paused_query "resume_${threshold}" "${threshold}" 1073741824
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${OWNER_PAUSE}"
    finish_query 0
done

echo "external aggregation"
# Eight rows per producer still alternate dictionaries and exercise backpressure,
# without forcing a spill for all 128 rows of the full fixture.
start_paused_query spill 1 1 "SELECT key, value, payload FROM ${TABLE_NAME} WHERE id < 8 ORDER BY id"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${OWNER_PAUSE}"
finish_query 0
SPILL_QUERY_ID="${QUERY_ID}"

echo "cancellation"
start_paused_query cancel 0 1073741824
# The owner is still paused and has released no bytes. Cancellation itself must wake the
# waiter before the owner is allowed to resume. Use a counter, not elapsed microseconds:
# a cancellation racing the initial predicate check can legitimately take zero microseconds.
wait_for_event AggregationDictionaryBackpressureCancelledWaits "KILL QUERY WHERE query_id = '${QUERY_ID}' ASYNC FORMAT Null;"
echo "cancellation woke the producer before the owner resumed"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${OWNER_PAUSE}"
finish_query 394

echo "owner exception"
start_paused_query exception 0 1073741824
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT ${OWNER_EXCEPTION}; SYSTEM DISABLE FAILPOINT ${OWNER_PAUSE}"
finish_query 710

echo "query after cancellation and exception"
${CLICKHOUSE_CLIENT} -q "
    SYSTEM DISABLE FAILPOINT ${OWNER_EXCEPTION};
    $(aggregation_query "${ORDERED_INPUT}" 0 1073741824 1)
    SYSTEM FLUSH LOGS query_log;
    SELECT 'external aggregation wrote a part', countIf(ProfileEvents['ExternalAggregationWritePart'] > 0) = 1
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${SPILL_QUERY_ID}' AND type = 'QueryFinish';
"
