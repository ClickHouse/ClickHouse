#!/usr/bin/env bash
# Tags: no-parallel
# The owner pause affects all dictionary-shard queries in this server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_NAME="low_cardinality_dictionary_memory_backpressure"
TEST_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/dictionary_memory_backpressure.XXXXXX")
OWNER_PAUSE="dictionary_aggregation_before_drain"
QUERY_ID="dictionary_memory_backpressure_${CLICKHOUSE_DATABASE}_$$"
QUERY_PID=""

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${OWNER_PAUSE}" ||:
    ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '${QUERY_ID}' ASYNC FORMAT Null" ||:
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
    SET max_threads = 1, max_insert_threads = 1;
    CREATE TABLE ${TABLE_NAME}
    (
        part UInt8,
        id UInt64,
        key LowCardinality(String)
    )
    ENGINE = MergeTree PARTITION BY part ORDER BY id
    SETTINGS index_granularity = 1, index_granularity_bytes = 0,
        min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, min_level_for_wide_part = 0;
    INSERT INTO ${TABLE_NAME} VALUES (0, 0, 'shared');
    INSERT INTO ${TABLE_NAME} SELECT 1, number + 1, 'shared' FROM numbers(128);
    SELECT throwIf(count() != 2 OR countIf(part_type = 'Wide') != 2
        OR countIf(rows = 1) != 1 OR countIf(rows = 128) != 1,
        'Expected one one-row and one 128-row Wide part')
    FROM system.parts WHERE database = currentDatabase() AND table = '${TABLE_NAME}' AND active FORMAT Null;
"

# Each producer starts with a local dictionary, then switches to its second dictionary.
# The two reads have independent dictionary caches, so do not rely on them sharing a shard.
# The first shard-owned block holds 9 MiB, exceeding the 8 MiB memory-derived watermark
# while its owner is paused. Subsequent blocks hold 1 MiB. The other producer must wait
# even when it drains its own shard, because owner-held input counts towards the same budget.
# Arrays are generated after the ordered read; `uniqExactArray` retains only two values.
ORDERED_INPUT="SELECT key, id FROM ${TABLE_NAME} ORDER BY id"
aggregation_query()
{
    local inputs="$1" key="$2" threads="$3" threshold="$4"
    echo "
        SELECT ${key} AS k, count(), uniqExactArray(arrayWithConstant(if(id = 1, 9 * 131072, 131072), toUInt64(id % 2)))
        FROM (${inputs}) GROUP BY k
        SETTINGS max_threads = ${threads}, max_threads_min_free_memory_per_thread = 0,
            max_memory_usage = 67108864, memory_overcommit_ratio_denominator = 0,
            enable_parallel_replicas = 0, serialize_query_plan = 0,
            max_streams_for_merge_tree_reading = 1, max_block_size = 1, preferred_block_size_bytes = 0,
            max_streams_for_union_step = 0, max_streams_for_union_step_to_max_threads_ratio = 0,
            merge_tree_use_deserialization_prefixes_cache = 1,
            optimize_read_in_order = 1, query_plan_remove_redundant_sorting = 0,
            query_plan_lift_up_union = 0, optimize_aggregation_in_order = 0,
            allow_aggregate_partitions_independently = 0, force_aggregate_partitions_independently = 0,
            enable_adaptive_aggregator = 0, enable_lazy_columns_replication = 0,
            collect_hash_table_stats_during_aggregation = 0, compile_aggregate_expressions = 0,
            max_rows_to_group_by = 0, group_by_two_level_threshold = ${threshold},
            group_by_two_level_threshold_bytes = 0, max_bytes_before_external_group_by = 0,
            max_bytes_ratio_before_external_group_by = 0, use_query_cache = 0,
            max_execution_time = 30, timeout_overflow_mode = 'throw',
            log_queries = 1, log_queries_probability = 1, log_profile_events = 1,
            log_queries_min_query_duration_ms = 0, log_queries_min_type = 'QUERY_FINISH';
    "
}

echo "decoded control"
${CLICKHOUSE_CLIENT} -q "$(aggregation_query "${ORDERED_INPUT} UNION ALL ${ORDERED_INPUT}" 'CAST(key AS String)' 2 0)"

echo "single producer"
${CLICKHOUSE_CLIENT} -q "$(aggregation_query "${ORDERED_INPUT}" key 1 0)"

for threshold in 0 1; do
    echo "two-level threshold ${threshold}"
    QUERY_ID="dictionary_memory_backpressure_${CLICKHOUSE_DATABASE}_$$_${threshold}"
    # Only pause the owner: do not enable either failpoint that lowers the watermark.
    ${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT ${OWNER_PAUSE}"
    ${CLICKHOUSE_CLIENT} --query_id="${QUERY_ID}" -q "$(aggregation_query "${ORDERED_INPUT} UNION ALL ${ORDERED_INPUT}" key 2 "${threshold}")" >"${TEST_DIR}/stdout" 2>"${TEST_DIR}/stderr" &
    QUERY_PID=$!
    # `CLICKHOUSE_CLIENT` contains the executable and its options.
    # shellcheck disable=SC2086
    timeout 30 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT ${OWNER_PAUSE} PAUSE"

    observed=0
    for ((attempt = 0; attempt < 100; ++attempt)); do
        observed=$(${CLICKHOUSE_CLIENT} -q "SELECT countIf(ProfileEvents['AggregationDictionaryBackpressureWaits'] > 0) > 0 FROM system.processes WHERE query_id = '${QUERY_ID}'")
        if [[ "${observed}" == 1 ]]; then break; fi
        sleep 0.05
    done
    if [[ "${observed}" != 1 ]]; then
        ${CLICKHOUSE_CLIENT} -q "
            SELECT read_rows, memory_usage,
                ProfileEvents['AggregationSingleLowCardinalityDictionarySwitches'] AS dictionary_switches,
                ProfileEvents['AggregationDictionaryBackpressureWaits'] AS backpressure_waits
            FROM system.processes WHERE query_id = '${QUERY_ID}' FORMAT Vertical;
        " >&2
        cat "${TEST_DIR}/stderr" >&2
        echo "No backpressure under the 64 MiB query memory limit" >&2
        exit 1
    fi
    echo "memory budget applied backpressure"

    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${OWNER_PAUSE}"
    timeout 30 tail --pid="${QUERY_PID}" --sleep-interval=0.05 -f /dev/null
    status=0
    wait "${QUERY_PID}" || status=$?
    QUERY_PID=""
    if [[ "${status}" != 0 ]]; then
        cat "${TEST_DIR}/stderr" >&2
        exit 1
    fi
    cat "${TEST_DIR}/stdout"

    ${CLICKHOUSE_CLIENT} -q "
        SYSTEM FLUSH LOGS query_log;
        SELECT count() = 1 AND countIf(memory_usage < 67108864
            AND ProfileEvents['AggregationSingleLowCardinalityDictionarySwitches'] > 0
            AND ProfileEvents['AggregationDictionaryBackpressureWaits'] > 0) = 1
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '${QUERY_ID}' AND type = 'QueryFinish';
    "
done
