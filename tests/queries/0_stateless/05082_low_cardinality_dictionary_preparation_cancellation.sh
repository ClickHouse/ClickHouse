#!/usr/bin/env bash
# Tags: no-parallel
# The preparation failpoints affect all dictionary-aggregation queries in this server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_NAME="low_cardinality_dictionary_preparation_cancellation"
TEST_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/dictionary_preparation_cancellation.XXXXXX")
MERGE_PAUSE="dictionary_aggregation_after_merge_task"
NORMALIZE_PAUSE="dictionary_aggregation_after_normalize_task"
QUERY_ID="dictionary_preparation_cancellation_${CLICKHOUSE_DATABASE}_$$"
QUERY_PID=""

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${MERGE_PAUSE}; SYSTEM DISABLE FAILPOINT ${NORMALIZE_PAUSE}" ||:
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
        'Expected two Wide parts with interleaved rows for dictionary pre-merging')
    FROM system.parts
    WHERE database = currentDatabase() AND table = '${TABLE_NAME}' AND active
    FORMAT Null;
"

# One-row ordered reads give a single producer dictionaries A, B, A. This forces a
# pre-merge for A, followed by two normalization tasks. The two-level pre-merge has
# 256 bucket tasks, so cancellation after the first task leaves unfinished sources.
# `groupArraySorted` exercises destruction of non-trivial aggregate states as well.
aggregation_query()
{
    local threshold="$1"
    echo "
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
            group_by_two_level_threshold = ${threshold},
            group_by_two_level_threshold_bytes = 0,
            max_bytes_before_external_group_by = 1073741824,
            max_bytes_ratio_before_external_group_by = 0;
    "
}

cancel_preparation()
{
    local failpoint="$1" threshold="$2" status=0
    QUERY_ID="dictionary_preparation_cancellation_${CLICKHOUSE_DATABASE}_$$_${failpoint}_${threshold}"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT ${failpoint}"
    ${CLICKHOUSE_CLIENT} --query_id="${QUERY_ID}" -q "$(aggregation_query "${threshold}")" >"${TEST_DIR}/stdout" 2>"${TEST_DIR}/stderr" &
    QUERY_PID=$!
    # `CLICKHOUSE_CLIENT` contains the executable and its options.
    # shellcheck disable=SC2086
    if ! timeout 30 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT ${failpoint} PAUSE"; then
        cat "${TEST_DIR}/stderr" >&2
        return 1
    fi

    # There is exactly one worker, so the observed pause is the only one to resume.
    # Keep the failpoint enabled: doing another task after cancellation pauses again
    # and prevents the query from finishing. Merely checking error 394 would miss the
    # bug, since a query that finishes all preparation work eventually reports it too.
    ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '${QUERY_ID}' ASYNC FORMAT Null; SYSTEM NOTIFY FAILPOINT ${failpoint}"
    if ! timeout 30 tail --pid="${QUERY_PID}" --sleep-interval=0.05 -f /dev/null; then
        echo "Cancelled query continued preparation after ${failpoint}" >&2
        cat "${TEST_DIR}/stderr" >&2
        return 1
    fi
    wait "${QUERY_PID}" || status=$?
    QUERY_PID=""
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${failpoint}"
    if [[ "${status}" == 0 ]] || ! grep -Fq 'Code: 394.' "${TEST_DIR}/stderr"; then
        cat "${TEST_DIR}/stderr" >&2
        echo "Expected query cancellation, got client status ${status}" >&2
        return 1
    fi
    echo "cancelled without continuing preparation"
}

echo "cancel dictionary pre-merge"
cancel_preparation "${MERGE_PAUSE}" 1

for threshold in 0 1; do
    echo "cancel normalization, two-level threshold ${threshold}"
    cancel_preparation "${NORMALIZE_PAUSE}" "${threshold}"

    echo "ordinary completion, two-level threshold ${threshold}"
    ${CLICKHOUSE_CLIENT} -q "$(aggregation_query "${threshold}")"
done
