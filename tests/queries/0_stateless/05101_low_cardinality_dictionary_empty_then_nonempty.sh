#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_NAME="low_cardinality_dictionary_empty_then_nonempty"
TRACE_FILE=$(mktemp "${CLICKHOUSE_TMP}/dictionary_empty_then_nonempty.XXXXXX")
trap '${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE_NAME}"; rm -f "${TRACE_FILE}"' EXIT
CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT//"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/--send_logs_level=trace}

${CLICKHOUSE_CLIENT} -q "
    SET max_threads = 1, max_insert_threads = 1, max_block_size = 16,
        low_cardinality_use_single_dictionary_for_part = 1;

    DROP TABLE IF EXISTS ${TABLE_NAME};
    CREATE TABLE ${TABLE_NAME}
    (
        id UInt64,
        key LowCardinality(String),
        arr Array(UInt64)
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS
        index_granularity = 8192,
        index_granularity_bytes = '10Mi',
        min_rows_for_wide_part = 2,
        min_bytes_for_wide_part = 0,
        min_level_for_wide_part = 0;

    SYSTEM STOP MERGES ${TABLE_NAME};
    INSERT INTO ${TABLE_NAME} VALUES (0, 'unused', []);

    ALTER TABLE ${TABLE_NAME} MODIFY SETTING min_rows_for_wide_part = 0;
    INSERT INTO ${TABLE_NAME} VALUES
        (1, 'left', [10, 20]),
        (2, 'right', [100, 200, 300]);

    SELECT throwIf(
        count() != 2
            OR countIf(part_type = 'Compact' AND rows = 1) != 1
            OR countIf(part_type = 'Wide' AND rows = 2) != 1,
        'Expected an empty-array Compact part followed by a populated Wide part')
    FROM system.parts
    WHERE database = currentDatabase() AND table = '${TABLE_NAME}' AND active
    FORMAT Null;
"

${CLICKHOUSE_CLIENT_TRACE} -q "
    SELECT key, count(), sum(value), groupArraySorted(3)(value)
    FROM
    (
        SELECT key, arr
        FROM ${TABLE_NAME}
        ORDER BY id
    )
    ARRAY JOIN arr AS value
    GROUP BY key
    ORDER BY key
    SETTINGS
        max_threads = 1,
        max_streams_for_merge_tree_reading = 1,
        max_block_size = 16,
        preferred_block_size_bytes = 0,
        merge_tree_use_deserialization_prefixes_cache = 1,
        optimize_read_in_order = 1,
        query_plan_remove_redundant_sorting = 0,
        optimize_aggregation_in_order = 0,
        enable_adaptive_aggregator = 0,
        collect_hash_table_stats_during_aggregation = 0,
        compile_aggregate_expressions = 0,
        max_rows_to_group_by = 0,
        group_by_two_level_threshold = 0,
        group_by_two_level_threshold_bytes = 0;
" 2>"${TRACE_FILE}" || {
    query_status=$?
    cat "${TRACE_FILE}" >&2
    exit "${query_status}"
}

awk '
    /Aggregator: Aggregation method: low_cardinality_single_dictionary$/ { index_tables++ }
    /Aggregator: Aggregation method: low_cardinality_key_string$/ { value_tables++ }
    /AggregatingTransform: Aggregated\. 5 to 2 rows/ { producers++ }
    END {
        printf "index tables: %d, value tables: %d, producers: %d\n", index_tables, value_tables, producers
    }
' "${TRACE_FILE}"
