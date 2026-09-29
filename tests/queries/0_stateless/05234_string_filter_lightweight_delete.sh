#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
# Reader-work assertions require fixed read plans, legacy serialization, and local ProfileEvents.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
set -e -o pipefail

opts=(
    --enable_analyzer 1
    --enable_multiple_prewhere_read_steps 1
    --optimize_functions_to_subcolumns 1
    --optimize_move_to_prewhere 0
    --optimize_prewhere_after_pushdown 0
    --apply_mutations_on_fly 0
    --apply_deleted_mask 1
    --use_query_condition_cache 0
    --use_query_cache 0
    --use_skip_indexes 0
    --log_queries 1
    --log_queries_probability 1
    --log_queries_min_query_duration_ms 0
    --log_profile_events 1
    --max_threads 1
)

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS string_filter_lightweight_delete;
    CREATE TABLE string_filter_lightweight_delete (id UInt64, gate UInt8, s String)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        serialization_info_version = 'with_types',
        string_serialization_version = 'single_stream',
        ratio_of_defaults_for_sparse_serialization = 1.0;
    INSERT INTO string_filter_lightweight_delete
    SELECT number, 1, concat('value_', toString(number)) FROM numbers(16384);
    DELETE FROM string_filter_lightweight_delete WHERE id % 2 = 0
    SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 2;
    SYSTEM STOP MERGES string_filter_lightweight_delete;

    -- Prove that deleted rows remain on disk and are filtered by the deletion mask.
    SELECT 'deletion mask', count() = 16384, countIf(_row_exists = 0) = 8192
    FROM string_filter_lightweight_delete SETTINGS apply_deleted_mask = 0;
"

prefix="string_filter_lwd_${CLICKHOUSE_DATABASE}_${RANDOM}"
for shape in main steps; do
    suffix=''
    if [[ "$shape" == steps ]]; then
        # Keep size and full-String consumers in non-adjacent PREWHERE steps.
        suffix="AND gate = 1 AND startsWith(s, 'value_')"
    fi
    for mode in control rewritten explicit; do
        predicate='notEmpty(s)'
        optimize=0
        if [[ "$mode" == rewritten ]]; then
            optimize=1
        elif [[ "$mode" == explicit ]]; then
            predicate='s.size > 0'
        fi
        echo "$shape $mode"
        ${CLICKHOUSE_CLIENT} "${opts[@]}" --optimize_string_size_subcolumn_with_full_read "$optimize" \
            --query_id "${prefix}_${shape}_${mode}" -q "
            SELECT count() = 8192, uniqExact(s) = 8192
            FROM string_filter_lightweight_delete PREWHERE ${predicate} ${suffix}
        "
    done

    # Include main-reader work: the full String may be read after PREWHERE.
    # Compare with the control rather than assuming a particular granule size.
    ${CLICKHOUSE_CLIENT} -q "
        SYSTEM FLUSH LOGS query_log;
        WITH ProfileEvents['RowsReadByPrewhereReaders'] + ProfileEvents['RowsReadByMainReader'] AS reader_rows
        SELECT '${shape} read work', count() = 3, min(reader_rows) > 0 AND min(reader_rows) = max(reader_rows)
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
            AND query_id IN ('${prefix}_${shape}_control', '${prefix}_${shape}_rewritten', '${prefix}_${shape}_explicit');
    "
done

${CLICKHOUSE_CLIENT} -q 'DROP TABLE string_filter_lightweight_delete'
