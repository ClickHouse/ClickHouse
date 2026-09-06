#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_NAME="low_cardinality_dictionary_retirement_revisit"
QUERY_ID="dictionary_retirement_revisit_${CLICKHOUSE_DATABASE}_$$"
trap '${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE_NAME}"' EXIT

# One part per partition prevents background merging. An ordered read interleaves
# their rows as A, B, C, A, B, C, ... with different index assignments for the same keys.
${CLICKHOUSE_CLIENT} -q "
    SET max_threads = 1, max_insert_threads = 1,
        max_block_size = 12, max_insert_block_size = 12,
        low_cardinality_use_single_dictionary_for_part = 1;

    DROP TABLE IF EXISTS ${TABLE_NAME};
    CREATE TABLE ${TABLE_NAME}
    (
        p UInt8,
        id UInt8,
        k LowCardinality(Nullable(String)),
        value UInt64,
        payload String
    )
    ENGINE = MergeTree
    PARTITION BY p
    ORDER BY id
    SETTINGS
        index_granularity = 1,
        index_granularity_bytes = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0,
        min_level_for_wide_part = 0;

    INSERT INTO ${TABLE_NAME} VALUES
        (0, 0, 'shared', 10, 'alpha'), (0, 3, NULL, 40, 'alpha'),
        (0, 6, 'other', 70, 'alpha'), (0, 9, 'shared', 100, 'alpha');
    INSERT INTO ${TABLE_NAME} VALUES
        (1, 1, 'other', 20, 'beta'), (1, 4, 'shared', 50, 'beta'),
        (1, 7, NULL, 80, 'beta'), (1, 10, 'shared', 110, 'beta');
    INSERT INTO ${TABLE_NAME} VALUES
        (2, 2, NULL, 30, 'alpha'), (2, 5, 'other', 60, 'alpha'),
        (2, 8, 'shared', 90, 'alpha'), (2, 11, 'shared', 120, 'gamma');

    SELECT throwIf(count() != 3 OR countIf(part_type = 'Wide' AND rows = 4) != 3,
        'Expected three Wide parts with four rows each')
    FROM system.parts
    WHERE database = currentDatabase() AND table = '${TABLE_NAME}' AND active
    FORMAT Null;
"

QUERIES=""
append_case()
{
    local name="$1" key_expression="$2" aggregates="$3" two_level_threshold="$4" spill_bytes="$5"
    QUERIES+="
        SELECT '${name}';
        SELECT /* case:${name} */ ${key_expression} AS key ${aggregates}
        FROM
        (
            SELECT k, value, payload
            FROM ${TABLE_NAME}
            ORDER BY id
        )
        GROUP BY key
        ORDER BY key NULLS FIRST
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
            group_by_two_level_threshold = ${two_level_threshold},
            group_by_two_level_threshold_bytes = 0,
            max_bytes_before_external_group_by = ${spill_bytes},
            max_bytes_ratio_before_external_group_by = 0,
            temporary_files_buffer_size = 4096,
            aggregation_memory_efficient_merge_threads = 1,
            log_queries = 1,
            log_profile_events = 1,
            log_queries_probability = 1,
            log_queries_min_query_duration_ms = 0,
            log_queries_min_type = 'QUERY_FINISH';
    "
}

# Exercise owning aggregate states, duplicate values across retired dictionaries, and
# the nullable key's default dictionary index. Keep the arrays sorted independently of
# accumulation order so all these aggregates remain eligible for dictionary sharding.
AGGREGATES=", count(), sum(value), uniqExact(payload), groupArraySorted(20)(value), groupArraySorted(20)(payload)"
append_case decoded_control 'CAST(k AS Nullable(String))' "${AGGREGATES}" 1 1073741824
append_case single_level k "${AGGREGATES}" 0 1073741824
append_case two_level k "${AGGREGATES}" 1 1073741824

# Each dictionary registration gets only one row before retirement, so its index table
# cannot reach this two-key threshold. The producer's retired-result table must combine
# different keys, convert to two-level, and spill itself; otherwise no spill is initiated.
append_case retired_result_spill k "${AGGREGATES}" 2 1

# Inline counts and aggregation without aggregate functions have separate conversion paths.
append_case inline_count k ', count()' 1 1073741824
append_case keys_only k '' 1 1073741824

# Inline labels distinguish the aggregation queries from other statements in the batch.
# Filter by `query_id` before reading query text from the shared log.
${CLICKHOUSE_CLIENT} --query_id="${QUERY_ID}" -q "
    ${QUERIES}
    SELECT 'query paths: name, all parts selected, dictionary switches, two-level, spilled';
    SYSTEM FLUSH LOGS query_log;
    SELECT
        extract(query, '/[*] case:([a-z_]+) [*]/') AS case_name,
        ProfileEvents['SelectedParts'] = 3,
        ProfileEvents['AggregationSingleLowCardinalityDictionarySwitches'],
        ProfileEvents['AggregationConvertedToTwoLevel'] > 0,
        ProfileEvents['ExternalAggregationWritePart'] > 0
    FROM system.query_log
    PREWHERE current_database = currentDatabase() AND query_id = '${QUERY_ID}'
    WHERE type = 'QueryFinish' AND case_name != ''
    ORDER BY case_name;
"
