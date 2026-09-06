#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_NAME="low_cardinality_dictionary_shard_budget"
VIEW_NAME="${TABLE_NAME}_input"
QUERY_ID="dictionary_shard_budget_${CLICKHOUSE_DATABASE}_$$"
trap '${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS ${VIEW_NAME}; DROP TABLE IF EXISTS ${TABLE_NAME}"' EXIT

# Two ordered parts per lane force one dictionary switch in each independent input.
# Separate partitions prevent background merges without changing server-wide state.
# The memory regression is in the number of allocated two-level tables, not keys.
# Keep 32 lanes and enough keys to populate all 32 shards, with 1024 rows per part.
${CLICKHOUSE_CLIENT} -q "
    SET max_threads = 1, max_insert_threads = 1,
        low_cardinality_use_single_dictionary_for_part = 1,
        low_cardinality_max_dictionary_size = 8192,
        max_memory_usage = 0;

    DROP VIEW IF EXISTS ${VIEW_NAME};
    DROP TABLE IF EXISTS ${TABLE_NAME};
    CREATE TABLE ${TABLE_NAME}
    (
        lane UInt8,
        phase UInt8,
        seq UInt64,
        k LowCardinality(String),
        value UInt64
    )
    ENGINE = MergeTree
    PARTITION BY (lane, phase)
    ORDER BY seq
    SETTINGS index_granularity = 1024, index_granularity_bytes = 0,
        min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, min_level_for_wide_part = 0;

    CREATE VIEW ${VIEW_NAME} AS
    SELECT k, value FROM ${TABLE_NAME} WHERE lane = {lane:UInt64} ORDER BY seq;

    INSERT INTO ${TABLE_NAME}
    SELECT
        toUInt8(intDiv(number, 2048)),
        toUInt8(intDiv(number % 2048, 1024)) AS phase,
        number % 2048,
        leftPad(toString(if(phase = 0, number % 1024, 1023 - number % 1024)), 6, '0'),
        toUInt64(1)
    FROM numbers(65536)
    SETTINGS max_block_size = 65536, max_insert_block_size = 65536,
        min_insert_block_size_rows = 65536, min_insert_block_size_bytes = 0,
        max_partitions_per_insert_block = 64;

    SELECT throwIf(count() != 64 OR countIf(part_type = 'Wide' AND rows = 1024) != 64,
        'Expected 64 Wide parts with 1024 rows each')
    FROM system.parts
    WHERE database = currentDatabase() AND table = '${TABLE_NAME}' AND active
    FORMAT Null;
"

QUERIES=""
append_case()
{
    local name="$1" key_expression="$2" distinct_lanes="$3" two_level_threshold="$4"
    local inputs="" producer
    for ((producer = 0; producer < 32; ++producer)); do
        if [[ -n "${inputs}" ]]; then
            inputs+=" UNION ALL "
        fi
        inputs+="SELECT * FROM ${VIEW_NAME}(lane = $((producer % distinct_lanes)))"
    done

    # All inputs have the same keys. Holding one complete two-level table per shard
    # of every dictionary used to require over 1 GiB at just 32 producers. The spill
    # threshold is above the memory limit so spilling cannot conceal that growth.
    QUERIES+="
        SELECT '${name}';
        SELECT /* case:${name} */ count(), min(c), max(c), min(s), max(s)
        FROM
        (
            SELECT ${key_expression} AS key, count() AS c, sum(value) AS s
            FROM (${inputs})
            GROUP BY key
        )
        SETTINGS
            max_threads = 32,
            max_threads_min_free_memory_per_thread = 0,
            enable_parallel_replicas = 0,
            use_query_cache = 0,
            max_streams_for_merge_tree_reading = 1,
            max_streams_for_union_step = 0,
            max_streams_for_union_step_to_max_threads_ratio = 0,
            max_block_size = 1024,
            preferred_block_size_bytes = 0,
            max_read_buffer_size = 4096,
            max_read_buffer_size_local_fs = 4096,
            merge_tree_use_deserialization_prefixes_cache = 1,
            optimize_read_in_order = 1,
            query_plan_remove_redundant_sorting = 0,
            query_plan_lift_up_union = 0,
            optimize_aggregation_in_order = 0,
            enable_adaptive_aggregator = 0,
            collect_hash_table_stats_during_aggregation = 0,
            compile_expressions = 0,
            compile_aggregate_expressions = 0,
            max_rows_to_group_by = 0,
            group_by_two_level_threshold = ${two_level_threshold},
            group_by_two_level_threshold_bytes = 0,
            max_memory_usage = 536870912,
            max_untracked_memory = 0,
            max_bytes_before_external_group_by = 2147483648,
            max_bytes_ratio_before_external_group_by = 0,
            max_bytes_before_external_sort = 0,
            max_bytes_ratio_before_external_sort = 0,
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

append_case decoded 'CAST(k AS String)' 32 1
append_case index_single_level k 32 0
append_case index_private k 32 1
# Four producers per dictionary exercise growth while earlier chunks may still use
# a smaller shard count. Their overlapping partial groups must also be combined.
append_case index_shared k 8 1

# Inline labels distinguish the aggregation queries from other statements in the batch.
# Filter by `query_id` before reading query text from the shared log.
${CLICKHOUSE_CLIENT} --query_id="${QUERY_ID}" -q "
    ${QUERIES}
    SELECT 'query paths: name, finished, exception, all rows read, all parts selected, dictionary switches, two-level, spilled';
    SYSTEM FLUSH LOGS query_log;
    SELECT
        extract(query, '/[*] case:([a-z_]+) [*]/') AS case_name,
        type = 'QueryFinish',
        exception_code,
        read_rows = 65536,
        ProfileEvents['SelectedParts'] = 64,
        ProfileEvents['AggregationSingleLowCardinalityDictionarySwitches'],
        ProfileEvents['AggregationConvertedToTwoLevel'] > 0,
        ProfileEvents['ExternalAggregationWritePart'] > 0
    FROM system.query_log
    PREWHERE current_database = currentDatabase() AND query_id = '${QUERY_ID}'
    WHERE type != 'QueryStart' AND case_name != ''
    ORDER BY case_name;
"
