#!/usr/bin/env bash
# Tags: no-parallel
# The normalization pause affects all dictionary-aggregation queries in this server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_NAME="low_cardinality_dictionary_normalization_batch_cancellation"
TEST_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/dictionary_normalization_batch_cancellation.XXXXXX")
NORMALIZE_PAUSE="dictionary_aggregation_after_normalize_batch"
QUERY_ID="dictionary_normalization_batch_cancellation_${CLICKHOUSE_DATABASE}_$$"
QUERY_PID=""

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${NORMALIZE_PAUSE}" ||:
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

# Both dictionaries fit the writer's default limit. Each variant has 64 keys, so
# normalization needs several eight-key batches regardless of which variant goes first.
${CLICKHOUSE_CLIENT} -q "
    SET max_threads = 1, max_insert_threads = 1, max_block_size = 64, max_insert_block_size = 64;
    CREATE TABLE ${TABLE_NAME}
    (
        part UInt8,
        id UInt64,
        k LowCardinality(String)
    )
    ENGINE = MergeTree PARTITION BY part ORDER BY id
    SETTINGS index_granularity = 8, index_granularity_bytes = 0,
        min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, min_level_for_wide_part = 0;
    INSERT INTO ${TABLE_NAME} SELECT 0, number + 1, toString(number) FROM numbers(64);
    INSERT INTO ${TABLE_NAME} SELECT 1, number + 65, toString(63 - number) FROM numbers(64);
    SELECT throwIf(count() != 2 OR countIf(part_type = 'Wide' AND rows = 64) != 2,
        'Expected two Wide parts with 64 rows each')
    FROM system.parts WHERE database = currentDatabase() AND table = '${TABLE_NAME}' AND active FORMAT Null;
"

aggregation_query()
{
    local threshold="$1"
    echo "
        SELECT count(), sum(n), sum(s), sum(arraySum(a))
        FROM
        (
            SELECT k, count() AS n, sum(id) AS s, groupArraySorted(2)(id) AS a
            FROM (SELECT k, id FROM ${TABLE_NAME} ORDER BY id)
            GROUP BY k
        )
        SETTINGS max_threads = 1, enable_parallel_replicas = 0, max_block_size = 8,
            preferred_block_size_bytes = 0, merge_tree_use_deserialization_prefixes_cache = 1,
            optimize_read_in_order = 1, query_plan_remove_redundant_sorting = 0,
            optimize_aggregation_in_order = 0, enable_adaptive_aggregator = 0,
            collect_hash_table_stats_during_aggregation = 0, compile_aggregate_expressions = 0,
            max_rows_to_group_by = 0, group_by_two_level_threshold = ${threshold},
            group_by_two_level_threshold_bytes = 0, max_bytes_before_external_group_by = 1073741824,
            max_bytes_ratio_before_external_group_by = 0, max_execution_time = 30;
    "
}

cancel_normalization()
{
    local threshold="$1" status=0
    QUERY_ID="dictionary_normalization_batch_cancellation_${CLICKHOUSE_DATABASE}_$$_${threshold}"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT ${NORMALIZE_PAUSE}"
    ${CLICKHOUSE_CLIENT} --query_id="${QUERY_ID}" -q "$(aggregation_query "${threshold}")" >"${TEST_DIR}/stdout" 2>"${TEST_DIR}/stderr" &
    QUERY_PID=$!
    # `CLICKHOUSE_CLIENT` contains the executable and its options.
    # shellcheck disable=SC2086
    if ! timeout 30 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT ${NORMALIZE_PAUSE} PAUSE"; then
        cat "${TEST_DIR}/stderr" >&2
        return 1
    fi

    # Keep the failpoint enabled: another batch would pause again. Checking only error
    # 394 would also pass if the query finished normalizing everything after cancellation.
    # `groupArraySorted` leaves non-trivial states on both sides of the partial transfer.
    ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '${QUERY_ID}' ASYNC FORMAT Null; SYSTEM NOTIFY FAILPOINT ${NORMALIZE_PAUSE}"
    if ! timeout 30 tail --pid="${QUERY_PID}" --sleep-interval=0.05 -f /dev/null; then
        echo "Cancelled query processed another normalization batch" >&2
        cat "${TEST_DIR}/stderr" >&2
        return 1
    fi
    wait "${QUERY_PID}" || status=$?
    QUERY_PID=""
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${NORMALIZE_PAUSE}"
    if [[ "${status}" == 0 ]] || ! grep -Fq 'Code: 394.' "${TEST_DIR}/stderr"; then
        cat "${TEST_DIR}/stderr" >&2
        echo "Expected query cancellation, got client status ${status}" >&2
        return 1
    fi
    echo "cancelled without processing another batch"
}

for threshold in 0 1; do
    echo "cancel between normalization batches, two-level threshold ${threshold}"
    cancel_normalization "${threshold}"
    echo "ordinary completion, two-level threshold ${threshold}"
    ${CLICKHOUSE_CLIENT} -q "$(aggregation_query "${threshold}")"
done
