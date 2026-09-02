#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_NAME="low_cardinality_dictionary_table_retention"
QUERY_PREFIX="dictionary_table_retention_${CLICKHOUSE_DATABASE}_$$_"
ERROR_FILE=$(mktemp "${CLICKHOUSE_TMP}/dictionary_table_retention.XXXXXX")
trap '${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE_NAME}"; rm -f "${ERROR_FILE}"' EXIT

# One part per partition prevents background merges without a global failpoint or a
# scheduling-dependent insert loop. The two inserts use opposite dictionary key orders.
${CLICKHOUSE_CLIENT} -q "
    SET max_threads = 1, max_insert_threads = 1,
        max_block_size = 64, max_insert_block_size = 64,
        max_partitions_per_insert_block = 64,
        low_cardinality_use_single_dictionary_for_part = 1,
        max_memory_usage = 0;

    DROP TABLE IF EXISTS ${TABLE_NAME};
    CREATE TABLE ${TABLE_NAME}
    (
        part UInt16,
        k LowCardinality(String),
        value UInt64
    )
    ENGINE = MergeTree
    PARTITION BY part
    ORDER BY tuple()
    SETTINGS
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0,
        min_level_for_wide_part = 0;

    INSERT INTO ${TABLE_NAME}
    SELECT toUInt16(intDiv(number, 2) * 2),
        if(number % 2 = 0, 'left', 'right') AS key,
        if(key = 'left', 1, 10)
    FROM numbers(64);

    INSERT INTO ${TABLE_NAME}
    SELECT toUInt16(intDiv(number, 2) * 2 + 1),
        if(number % 2 = 0, 'right', 'left') AS key,
        if(key = 'left', 1, 10)
    FROM numbers(64);

    SELECT throwIf(count() != 64 OR countIf(part_type = 'Wide' AND rows = 2) != 64,
        'Expected 64 Wide parts with two rows each')
    FROM system.parts
    WHERE database = currentDatabase() AND table = '${TABLE_NAME}' AND active
    FORMAT Null;
"

QUERIES=""
append_case()
{
    local name="$1" key_expression="$2" two_level_threshold="$3" spill_bytes="$4" part_filter="${5:-}"

    # Two global keys, two rows per input chunk, and one producer: neither useful
    # aggregate states nor queued input can explain growth proportional to the parts.
    # The decoded control has exactly the same input and memory limit. A high nonzero
    # spill threshold permits two-level aggregation with one producer but cannot spill
    # before the 32 MiB memory limit; the one-byte threshold must actually spill.
    # Pin aggregate compilation and temporary-file buffers: the harness randomizes
    # these, but neither is needed to reproduce retained-table growth.
    QUERIES+="
        SELECT '${name}';
        SELECT /* case:${name} */ ${key_expression} AS key, count(), sum(value)
        FROM ${TABLE_NAME}
        ${part_filter}
        GROUP BY key ORDER BY key
        SETTINGS
            max_threads = 1,
            max_block_size = 2,
            preferred_block_size_bytes = 0,
            max_read_buffer_size = 4096,
            max_read_buffer_size_local_fs = 4096,
            merge_tree_use_deserialization_prefixes_cache = 1,
            optimize_read_in_order = 0,
            optimize_aggregation_in_order = 0,
            enable_adaptive_aggregator = 0,
            collect_hash_table_stats_during_aggregation = 0,
            compile_aggregate_expressions = 0,
            max_rows_to_group_by = 0,
            group_by_two_level_threshold = ${two_level_threshold},
            group_by_two_level_threshold_bytes = 0,
            max_bytes_before_external_group_by = ${spill_bytes},
            max_bytes_ratio_before_external_group_by = 0,
            aggregation_memory_efficient_merge_threads = 1,
            temporary_files_buffer_size = 4096,
            max_memory_usage = 33554432,
            max_untracked_memory = 0,
            memory_overcommit_ratio_denominator = 0,
            memory_overcommit_ratio_denominator_for_user = 0,
            memory_usage_overcommit_max_wait_microseconds = 0,
            log_queries = 1,
            log_profile_events = 1,
            log_queries_probability = 1,
            log_queries_min_query_duration_ms = 0,
            log_queries_min_type = 'QUERY_FINISH';
    "
}

run_cases()
{
    local name="$1" queries="${QUERIES}"
    QUERIES=""
    if ${CLICKHOUSE_CLIENT} --query_id="${QUERY_PREFIX}${name}" -q "${queries}" 2>"${ERROR_FILE}"; then
        return 0
    fi

    echo "${name} failed:" >&2
    cat "${ERROR_FILE}" >&2
    return 1
}

# These controls must pass before attributing an error to dictionary-table retention.
# Keep all 64 parts for retention checks. Eight parts cover both dictionary orders
# for the spill controls without producing 64 tiny spill files per query.
append_case decoded_no_spill 'CAST(k AS String)' 1 1073741824
append_case decoded_spill 'CAST(k AS String)' 1 1 'WHERE part < 8'
append_case index_single_level k 0 1073741824
run_cases controls

# Both are expected to succeed after bounding retained dictionary tables. Do not bless
# `MEMORY_LIMIT_EXCEEDED` as an expected error: it is the regression being reproduced.
# Run both arms even on the broken implementation so their failures are visible together.
failed=0
append_case index_no_spill k 1 1073741824
run_cases index_no_spill || failed=1
append_case index_spill k 1 1 'WHERE part < 8'
run_cases index_spill || failed=1

# Pin the paths, not exact allocation sizes or spill counts. In particular, this catches
# silently falling back to decoded keys or disabling two-level/external aggregation.
echo "query paths: name, finished, exception, selected parts, dictionary switches, two-level, spilled"
${CLICKHOUSE_CLIENT} -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT
        extract(query, '/[*] case:([a-z_]+) [*]/') AS case_name,
        type = 'QueryFinish',
        exception_code,
        ProfileEvents['SelectedParts'],
        ProfileEvents['AggregationSingleLowCardinalityDictionarySwitches'] > 0,
        ProfileEvents['AggregationConvertedToTwoLevel'] > 0,
        ProfileEvents['ExternalAggregationWritePart'] > 0
    FROM system.query_log
    PREWHERE current_database = currentDatabase()
        AND startsWith(query_id, '${QUERY_PREFIX}')
    WHERE type != 'QueryStart' AND case_name != ''
    ORDER BY case_name;
"

exit "${failed}"
