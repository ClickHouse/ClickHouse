#!/usr/bin/env bash
# Tags: no-parallel
# The owner failpoint affects all dictionary-shard queries in this server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_NAME="low_cardinality_dictionary_drain_cancellation"
TEST_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/dictionary_drain_cancellation.XXXXXX")
OWNER_PAUSE="dictionary_aggregation_before_drain"
QUERY_ID="dictionary_drain_cancellation_${CLICKHOUSE_DATABASE}_$$"
QUERY_PID=""
QUERY_LOG_CHECKS=""

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '${QUERY_ID}' ASYNC FORMAT Null; SYSTEM DISABLE FAILPOINT ${OWNER_PAUSE}" ||:
    if [[ -n "${QUERY_PID}" ]]; then
        timeout 30 tail --pid="${QUERY_PID}" --sleep-interval=0.05 -f /dev/null ||:
        wait "${QUERY_PID}" ||:
    fi
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE_NAME}" ||:
    rm -f "${TEST_DIR}/stdout" "${TEST_DIR}/stderr"
    rmdir "${TEST_DIR}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -q "
    SET max_threads = 1, max_insert_threads = 1, max_block_size = 3, max_insert_block_size = 3,
        low_cardinality_use_single_dictionary_for_part = 1;
    DROP TABLE IF EXISTS ${TABLE_NAME};
    CREATE TABLE ${TABLE_NAME}
    (
        id UInt64,
        key LowCardinality(String),
        value UInt64
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS
        index_granularity = 1,
        index_granularity_bytes = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0,
        min_level_for_wide_part = 0;
    SYSTEM STOP MERGES ${TABLE_NAME};
    INSERT INTO ${TABLE_NAME} VALUES (0, 'shared', 10), (2, 'shared', 30);
    INSERT INTO ${TABLE_NAME} VALUES (1, 'shared', 20);
    SELECT throwIf(
        count() != 2 OR countIf(part_type = 'Wide') != 2
            OR countIf(rows = 2) != 1 OR countIf(rows = 1) != 1,
        'Expected two Wide parts with interleaved rows')
    FROM system.parts
    WHERE database = currentDatabase() AND table = '${TABLE_NAME}' AND active
    FORMAT Null;
"

# Ordered one-row reads give one producer dictionaries A, B, A. The first row
# aggregates locally; the switch to B creates a fresh shard and pauses its owner
# with an unprocessed block. Processing that block converts the new table to two
# levels and, with the one-byte spill threshold, writes a temporary part.
# Cancellation must leave those counters unchanged. Error 394 alone would also
# pass if the owner processed all its held input before noticing cancellation.
run_case()
{
    local mode="$1" spill_bytes="$2" status=0 cancel_query=""
    local snapshot process_count conversions_before spills_before
    QUERY_ID="dictionary_drain_cancellation_${CLICKHOUSE_DATABASE}_$$_${mode}_${spill_bytes}"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT ${OWNER_PAUSE}"
    ${CLICKHOUSE_CLIENT} --query_id="${QUERY_ID}" -q "
        SELECT key, count(), sum(value), groupArraySorted(10)(value)
        FROM (SELECT key, value FROM ${TABLE_NAME} ORDER BY id)
        GROUP BY key ORDER BY key
        SETTINGS
            max_threads = 1,
            enable_parallel_replicas = 0,
            max_block_size = 1,
            preferred_block_size_bytes = 0,
            merge_tree_use_deserialization_prefixes_cache = 1,
            optimize_read_in_order = 1,
            query_plan_remove_redundant_sorting = 0,
            optimize_aggregation_in_order = 0,
            enable_adaptive_aggregator = 0,
            collect_hash_table_stats_during_aggregation = 0,
            compile_aggregate_expressions = 0,
            max_rows_to_group_by = 0,
            group_by_two_level_threshold = 1,
            group_by_two_level_threshold_bytes = 0,
            max_bytes_before_external_group_by = ${spill_bytes},
            max_bytes_ratio_before_external_group_by = 0,
            aggregation_memory_efficient_merge_threads = 1,
            temporary_files_buffer_size = 4096,
            max_untracked_memory = 0,
            log_queries = 1,
            log_profile_events = 1,
            log_queries_probability = 1,
            log_queries_min_query_duration_ms = 0,
            log_queries_min_type = 'QUERY_FINISH';
    " >"${TEST_DIR}/stdout" 2>"${TEST_DIR}/stderr" &
    QUERY_PID=$!
    if [[ "${mode}" == cancel ]]; then
        cancel_query="KILL QUERY WHERE query_id = '${QUERY_ID}' ASYNC FORMAT Null;"
    fi
    # Capture the counters while paused, then cancel or resume on the same connection.
    # `CLICKHOUSE_CLIENT` contains the executable and its options.
    # shellcheck disable=SC2086
    if ! snapshot=$(timeout 30 ${CLICKHOUSE_CLIENT} -q "
        SYSTEM WAIT FAILPOINT ${OWNER_PAUSE} PAUSE;
        SELECT count(), sum(ProfileEvents['AggregationConvertedToTwoLevel']), sum(ProfileEvents['ExternalAggregationWritePart'])
        FROM system.processes WHERE query_id = '${QUERY_ID}';
        ${cancel_query}
        SYSTEM DISABLE FAILPOINT ${OWNER_PAUSE};
    "); then
        cat "${TEST_DIR}/stderr" >&2
        return 1
    fi
    IFS=$'\t' read -r process_count conversions_before spills_before <<< "${snapshot}"
    if [[ "${process_count}" != 1 ]] || ((conversions_before == 0)) \
        || { [[ "${spill_bytes}" == 1 ]] && ((spills_before == 0)); } \
        || { [[ "${spill_bytes}" != 1 ]] && ((spills_before != 0)); }; then
        echo "Unexpected counters before draining: ${snapshot}" >&2
        return 1
    fi

    if ! timeout 30 tail --pid="${QUERY_PID}" --sleep-interval=0.05 -f /dev/null; then
        cat "${TEST_DIR}/stderr" >&2
        return 1
    fi
    wait "${QUERY_PID}" || status=$?
    QUERY_PID=""

    if [[ "${mode}" == cancel ]]; then
        if [[ "${status}" == 0 ]] || ! grep -Fq 'Code: 394.' "${TEST_DIR}/stderr"; then
            cat "${TEST_DIR}/stderr" >&2
            echo "Expected query cancellation, got client status ${status}" >&2
            return 1
        fi
        QUERY_LOG_CHECKS+="
            SELECT '${mode}', ${spill_bytes}, count() = 1 AND countIf(
                exception_code = 394
                AND ProfileEvents['AggregationConvertedToTwoLevel'] = ${conversions_before}
                AND ProfileEvents['ExternalAggregationWritePart'] = ${spills_before}) = 1 AS stopped_without_draining
            FROM system.query_log
            WHERE current_database = currentDatabase() AND query_id = '${QUERY_ID}' AND type = 'ExceptionWhileProcessing';
        "
    else
        if [[ "${status}" != 0 ]]; then
            cat "${TEST_DIR}/stderr" >&2
            return 1
        fi
        cat "${TEST_DIR}/stdout"
        # The same paused query must do observable work when resumed without cancellation.
        QUERY_LOG_CHECKS+="
            SELECT '${mode}', ${spill_bytes}, count() = 1 AND countIf(
                ProfileEvents['AggregationConvertedToTwoLevel'] > ${conversions_before}
                AND if(${spill_bytes} = 1,
                    ProfileEvents['ExternalAggregationWritePart'] > ${spills_before},
                    ProfileEvents['ExternalAggregationWritePart'] = 0)) = 1 AS resumed_drain_did_work
            FROM system.query_log
            WHERE current_database = currentDatabase() AND query_id = '${QUERY_ID}' AND type = 'QueryFinish';
        "
    fi
}

for spill_bytes in 1073741824 1; do
    echo "cancel owner, spill threshold ${spill_bytes}"
    run_case cancel "${spill_bytes}"
    echo "resume owner, spill threshold ${spill_bytes}"
    run_case resume "${spill_bytes}"
done

echo "query log checks"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log; ${QUERY_LOG_CHECKS}"
