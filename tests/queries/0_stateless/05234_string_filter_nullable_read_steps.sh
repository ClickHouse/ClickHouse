#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
# The assertions count rows read by PREWHERE readers, not elapsed time.
# Parallel replicas do not report all replica-side ProfileEvents to the coordinator.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
set -e -o pipefail

opts=(
    --enable_analyzer 1
    --enable_multiple_prewhere_read_steps 1
    --optimize_functions_to_subcolumns 0
    --optimize_move_to_prewhere 0
    --optimize_prewhere_after_pushdown 0
    --use_query_condition_cache 0
    --use_query_cache 0
    --use_skip_indexes 0
    --log_queries 1
    --log_queries_probability 1
    --log_profile_events 1
    --max_threads 1
)

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS string_filter_nullable_steps;
    CREATE TABLE string_filter_nullable_steps
    (
        id UInt64,
        part UInt8,
        gate UInt8,
        ns Nullable(String),
        t Tuple(value Nullable(String))
    ) ENGINE = MergeTree PARTITION BY part ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        serialization_info_version = 'with_types',
        propagate_types_serialization_versions_to_nested_types = 1,
        string_serialization_version = 'single_stream',
        ratio_of_defaults_for_sparse_serialization = 1.0;

    INSERT INTO string_filter_nullable_steps
    SELECT number, 0, 1, concat('value_', toString(number)), tuple(concat('value_', toString(number)))
    FROM numbers(16384);

    ALTER TABLE string_filter_nullable_steps MODIFY SETTING string_serialization_version = 'with_size_stream';
    INSERT INTO string_filter_nullable_steps
    SELECT number, 1, 1, concat('value_', toString(number)), tuple(concat('value_', toString(number)))
    FROM numbers(16384);

    -- Both old and new parts must resolve their original physical names after the rename.
    ALTER TABLE string_filter_nullable_steps RENAME COLUMN ns TO renamed_ns, RENAME COLUMN t TO renamed_t;
"

prefix="nullable_string_steps_${CLICKHOUSE_DATABASE}"
echo 'wrapped read results'
for column in renamed_ns renamed_t.value; do
    for layout in legacy modern mixed; do
        where_clause=''
        expected_rows=16384
        if [[ "$layout" == legacy ]]; then
            where_clause='WHERE part = 0'
        elif [[ "$layout" == modern ]]; then
            where_clause='WHERE part = 1'
        else
            expected_rows=32768
        fi

        # The gate keeps size and full-String predicates in non-adjacent steps.
        # All rows pass, so each additional reader contributes exactly one row per input row.
        ${CLICKHOUSE_CLIENT} "${opts[@]}" --query_id "${prefix}_${column}_${layout}" -q "
            SELECT count() = ${expected_rows}
            FROM string_filter_nullable_steps
            PREWHERE ${column}.size > 0 AND gate = 1 AND startsWith(${column}, 'value_')
            ${where_clause}
        "
    done
done

${CLICKHOUSE_CLIENT} -q "
    SYSTEM FLUSH LOGS query_log;
    -- Legacy: String/size together, then gate (2N). Modern: size, gate, String (3N).
    -- Mixed parts retain those per-part plans (5N), rather than co-reading modern strings.
    SELECT 'wrapped read steps', count() = 6,
        countIf(ProfileEvents['RowsReadByPrewhereReaders'] =
            16384 * multiIf(endsWith(query_id, '_legacy'), 2, endsWith(query_id, '_modern'), 3, 5)) = 6
    FROM system.query_log
    WHERE current_database = currentDatabase() AND startsWith(query_id, '${prefix}_') AND type = 'QueryFinish';
    DROP TABLE string_filter_nullable_steps;

    DROP TABLE IF EXISTS string_filter_nullable_values;
    CREATE TABLE string_filter_nullable_values
    (
        id UInt64,
        part UInt8,
        ns Nullable(String),
        t Tuple(value Nullable(String))
    ) ENGINE = MergeTree PARTITION BY part ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        serialization_info_version = 'with_types',
        propagate_types_serialization_versions_to_nested_types = 1,
        string_serialization_version = 'single_stream',
        ratio_of_defaults_for_sparse_serialization = 1.0;
    INSERT INTO string_filter_nullable_values VALUES (0, 0, '', ('')), (1, 0, NULL, (NULL)), (2, 0, 'legacy', ('legacy'));
    ALTER TABLE string_filter_nullable_values MODIFY SETTING string_serialization_version = 'with_size_stream';
    INSERT INTO string_filter_nullable_values VALUES (3, 1, '', ('')), (4, 1, NULL, (NULL)), (5, 1, 'modern', ('modern'));
"

for column in ns t.value; do
    echo "$column empty"
    ${CLICKHOUSE_CLIENT} "${opts[@]}" -q "
        SELECT id, isNull(${column}), length(${column})
        FROM string_filter_nullable_values PREWHERE ${column}.size = 0 ORDER BY id
    "
    echo "$column nonempty"
    ${CLICKHOUSE_CLIENT} "${opts[@]}" -q "
        SELECT id, ${column}, ${column}.size
        FROM string_filter_nullable_values PREWHERE ${column}.size > 0 ORDER BY id
    "
    echo "$column null"
    ${CLICKHOUSE_CLIENT} "${opts[@]}" -q "
        SELECT id, isNull(${column})
        FROM string_filter_nullable_values PREWHERE isNull(${column}.size) ORDER BY id
    "
done

${CLICKHOUSE_CLIENT} -q 'DROP TABLE string_filter_nullable_values'
