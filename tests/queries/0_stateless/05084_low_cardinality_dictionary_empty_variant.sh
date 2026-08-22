#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_NAME="low_cardinality_dictionary_empty_variant"
TRACE_FILE=$(mktemp "${CLICKHOUSE_TMP}/dictionary_empty_variant.XXXXXX")
trap '${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE_NAME}"; rm -f "${TRACE_FILE}"' EXIT
CLICKHOUSE_CLIENT_TRACE=$(echo "${CLICKHOUSE_CLIENT}" | sed "s/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=trace/g")

# `Compact` parts require adaptive granularity; keep `index_granularity_bytes` nonzero.
${CLICKHOUSE_CLIENT} -q "
    SET max_threads = 1, max_insert_threads = 1, max_block_size = 16,
        low_cardinality_use_single_dictionary_for_part = 1;

    DROP TABLE IF EXISTS ${TABLE_NAME};
    CREATE TABLE ${TABLE_NAME}
    (
        part UInt8,
        k LowCardinality(String),
        arr Array(UInt64)
    )
    ENGINE = MergeTree
    PARTITION BY part
    ORDER BY tuple()
    SETTINGS
        index_granularity = 8192,
        index_granularity_bytes = '10Mi',
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0,
        min_level_for_wide_part = 0;

    SYSTEM STOP MERGES ${TABLE_NAME};
    INSERT INTO ${TABLE_NAME} VALUES (0, 'left', [10, 20]), (0, 'right', [100, 200, 300]);

    ALTER TABLE ${TABLE_NAME} MODIFY SETTING min_rows_for_wide_part = 2;
    INSERT INTO ${TABLE_NAME} VALUES (1, 'unused', []);

    SELECT throwIf(
        count() != 2
            OR countIf(partition_id = '0' AND part_type = 'Wide' AND rows = 2) != 1
            OR countIf(partition_id = '1' AND part_type = 'Compact' AND rows = 1) != 1,
        'Expected a populated Wide part and a Compact part with an empty array')
    FROM system.parts
    WHERE database = currentDatabase() AND table = '${TABLE_NAME}' AND active
    FORMAT Null;
"

# Give each part its own one-stream read through `UNION ALL`. A shared read pool
# could assign both parts to one producer and normalize its table during consumption,
# hiding the incompatible methods at the final merge. Do not narrow the union.
# `ARRAY JOIN` leaves the `Compact` producer with an initialized, zero-row value-key
# table, while the `Wide` producer has a populated dictionary-index table.
for key_mode in decoded index; do
    key_expression="k"
    if [[ "${key_mode}" == decoded ]]; then
        key_expression="CAST(k AS String)"
    fi

    for two_level_threshold in 0 1; do
        echo "${key_mode}, two-level threshold ${two_level_threshold}"
        ${CLICKHOUSE_CLIENT_TRACE} -q "
            SELECT ${key_expression} AS key, count(), sum(value), groupArraySorted(3)(value)
            FROM
            (
                SELECT k, arr FROM ${TABLE_NAME} WHERE part = 0
                UNION ALL
                SELECT k, arr FROM ${TABLE_NAME} WHERE part = 1
            )
            ARRAY JOIN arr AS value
            GROUP BY key
            ORDER BY key
            SETTINGS
                max_threads = 2,
                max_threads_min_free_memory_per_thread = 0,
                enable_parallel_replicas = 0,
                max_streams_for_merge_tree_reading = 1,
                max_streams_for_union_step = 0,
                max_streams_for_union_step_to_max_threads_ratio = 0,
                max_block_size = 16,
                preferred_block_size_bytes = 0,
                merge_tree_use_deserialization_prefixes_cache = 1,
                query_plan_lift_up_union = 0,
                enable_lazy_columns_replication = 0,
                optimize_read_in_order = 0,
                optimize_aggregation_in_order = 0,
                enable_adaptive_aggregator = 0,
                empty_result_for_aggregation_by_empty_set = 0,
                collect_hash_table_stats_during_aggregation = 0,
                compile_aggregate_expressions = 0,
                max_rows_to_group_by = 0,
                group_by_two_level_threshold = ${two_level_threshold},
                group_by_two_level_threshold_bytes = 0,
                max_bytes_before_external_group_by = 1073741824,
                max_bytes_ratio_before_external_group_by = 0;
        " 2>"${TRACE_FILE}" || {
            query_status=$?
            cat "${TRACE_FILE}" >&2
            exit "${query_status}"
        }

        if [[ "${key_mode}" == index ]]; then
            # Check that the empty table was initialized, not merely an unused input,
            # and that the nonempty input still exercises dictionary-index aggregation.
            awk '
                /Aggregator: Aggregation method: low_cardinality_single_dictionary$/ { index_tables++ }
                /Aggregator: Aggregation method: low_cardinality_key_string$/ { value_tables++ }
                /AggregatingTransform: Aggregated\. 0 to 0 rows/ { empty_producers++ }
                /AggregatingTransform: Aggregated\. 5 to 2 rows/ { nonempty_producers++ }
                END {
                    printf "index tables: %d, value tables: %d, empty producers: %d, nonempty producers: %d\n",
                        index_tables, value_tables, empty_producers, nonempty_producers
                }
            ' "${TRACE_FILE}"
        fi
    done
done
