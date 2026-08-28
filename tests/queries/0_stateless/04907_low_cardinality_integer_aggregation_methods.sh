#!/usr/bin/env bash
# Tags: no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

# Forced aggregation in order sorts the input and then aggregates a single key without a keyed hash
# table. Without in-order aggregation, every integer width must select its corresponding cached
# `LowCardinality` method. A second, nullable key exercises the dictionary's NULL entry in regular
# aggregation, and the two inserts keep overlapping values in separate part dictionaries.

types=(UInt8 UInt16 UInt32 UInt64 UInt128 UInt256)
methods=(
    low_cardinality_key8
    low_cardinality_key16
    low_cardinality_key32
    low_cardinality_key64
    low_cardinality_key128
    low_cardinality_key256
)

common_options=(
    --allow_suspicious_low_cardinality_types=1
    --enable_parallel_replicas=0
    --collect_hash_table_stats_during_aggregation=0
)

test_pid=$$
log_prefix="04907_lc_${test_pid}_"
spill_log_prefix="04907_lc_spill_${test_pid}_"

for i in "${!types[@]}"
do
    type="${types[$i]}"
    expected_method="${methods[$i]}"
    table="lc_integer_aggregation_04907_${type}"

    $CLICKHOUSE_CLIENT "${common_options[@]}" -q "DROP TABLE IF EXISTS ${table} SYNC"
    $CLICKHOUSE_CLIENT "${common_options[@]}" -q "
        CREATE TABLE ${table}
        (
            k LowCardinality(${type}),
            k_nullable LowCardinality(Nullable(${type})),
            v UInt64
        )
        ENGINE = MergeTree
        ORDER BY k"

    $CLICKHOUSE_CLIENT "${common_options[@]}" -q "SYSTEM STOP MERGES ${table}"
    $CLICKHOUSE_CLIENT "${common_options[@]}" --max_block_size=17 -q "
        INSERT INTO ${table}
        SELECT
            CAST(number % 31, '${type}'),
            if(number % 17 = 0, NULL, CAST(number % 31, '${type}')),
            number
        FROM numbers(200)"
    $CLICKHOUSE_CLIENT "${common_options[@]}" --max_block_size=19 -q "
        INSERT INTO ${table}
        SELECT
            CAST(number % 31, '${type}'),
            if(number % 19 = 0, NULL, CAST(number % 31, '${type}')),
            number + 200
        FROM numbers(200)"

    in_order_plan=$(
        $CLICKHOUSE_CLIENT "${common_options[@]}" -q "
            SELECT
                countIf(match(explain, '(^|[^A-Za-z])AggregatingInOrderTransform')) > 0
                AND countIf(explain LIKE '%FinishAggregatingInOrderTransform%') = 0
            FROM
            (
                EXPLAIN PIPELINE
                SELECT k, sum(v)
                FROM ${table}
                GROUP BY k
                ORDER BY k
                SETTINGS force_aggregation_in_order = 1, optimize_aggregation_in_order = 0,
                         optimize_read_in_order = 0, max_threads = 1
            )")
    printf '%s\tforced in-order plan\t%s\n' "$type" "$in_order_plan"

    regular_method=$(
        $CLICKHOUSE_CLIENT "${common_options[@]}" --send_logs_level=trace \
            --optimize_aggregation_in_order=0 --optimize_read_in_order=0 --max_threads=1 \
            -q "SELECT sum(v) FROM ${table} GROUP BY k FORMAT Null" 2>&1 \
            | grep -oE 'Aggregation method: [a-z_0-9]+' | sort -u)
    printf '%s\tregular method\t%s\n' "$type" "$regular_method"

    if [[ "$regular_method" != "Aggregation method: ${expected_method}" ]]
    then
        echo "Unexpected aggregation method for ${type}: ${regular_method}" >&2
        exit 1
    fi

    in_order_result=$(
        $CLICKHOUSE_CLIENT "${common_options[@]}" --force_aggregation_in_order=1 \
            --optimize_aggregation_in_order=0 --optimize_read_in_order=0 --max_threads=1 -q "
                SELECT ifNull(toString(k), 'NULL'), sum(v), count()
                FROM ${table}
                GROUP BY k
                ORDER BY k NULLS FIRST")
    regular_result=$(
        $CLICKHOUSE_CLIENT "${common_options[@]}" --optimize_aggregation_in_order=0 \
            --optimize_read_in_order=0 --max_threads=1 -q "
                SELECT ifNull(toString(k), 'NULL'), sum(v), count()
                FROM ${table}
                GROUP BY k
                ORDER BY k NULLS FIRST")

    if [[ "$in_order_result" != "$regular_result" ]]
    then
        echo "In-order and regular aggregation results differ for ${type}" >&2
        exit 1
    fi
    printf '%s\tresults equal\t1\n' "$type"

    nullable_method=$(
        $CLICKHOUSE_CLIENT "${common_options[@]}" --send_logs_level=trace \
            --optimize_aggregation_in_order=0 --optimize_read_in_order=0 --max_threads=1 \
            -q "SELECT sum(v) FROM ${table} GROUP BY k_nullable FORMAT Null" 2>&1 \
            | grep -oE 'Aggregation method: [a-z_0-9]+' | sort -u)
    printf '%s\tnullable regular method\t%s\n' "$type" "$nullable_method"

    if [[ "$nullable_method" != "Aggregation method: ${expected_method}" ]]
    then
        echo "Unexpected nullable aggregation method for ${type}: ${nullable_method}" >&2
        exit 1
    fi

    nullable_lc_result=$(
        $CLICKHOUSE_CLIENT "${common_options[@]}" --optimize_aggregation_in_order=0 \
            --optimize_read_in_order=0 --max_threads=1 -q "
                SELECT ifNull(toString(k_nullable), 'NULL'), sum(v), count()
                FROM ${table}
                GROUP BY k_nullable
                ORDER BY k_nullable NULLS FIRST")
    nullable_plain_result=$(
        $CLICKHOUSE_CLIENT "${common_options[@]}" --optimize_aggregation_in_order=0 \
            --optimize_read_in_order=0 --max_threads=1 -q "
                SELECT ifNull(toString(plain_key), 'NULL'), sum(v), count()
                FROM
                (
                    SELECT CAST(k_nullable, 'Nullable(${type})') AS plain_key, v
                    FROM ${table}
                )
                GROUP BY plain_key
                ORDER BY plain_key NULLS FIRST")

    if [[ "$nullable_lc_result" != "$nullable_plain_result" ]]
    then
        echo "LowCardinality and plain nullable aggregation results differ for ${type}" >&2
        exit 1
    fi
    printf '%s\tnullable results equal\t1\n' "$type"

    # Check two-level eligibility too. The compact 8- and 16-bit methods are intentionally
    # single-level; the cached methods from 32 through 256 bits have two-level counterparts.
    $CLICKHOUSE_CLIENT "${common_options[@]}" --optimize_aggregation_in_order=0 \
        --optimize_read_in_order=0 --max_threads=1 \
        --group_by_two_level_threshold=1 --group_by_two_level_threshold_bytes=0 \
        --max_bytes_before_external_group_by=1000000000 --max_bytes_ratio_before_external_group_by=0 \
        --log_comment="${log_prefix}${type}" \
        -q "SELECT sum(v) FROM ${table} GROUP BY k FORMAT Null"
done

# Exercise `mergeBlocks` for the new wide cached methods. Keep the two source parts separate so
# their LowCardinality dictionaries are independent, compare the spilled results with the same
# two-level aggregation kept in memory, and record each spill in query_log below. For nullable keys,
# also assert the intentional external-merge choice: small LowCardinality dictionaries stay on the
# numeric fast path rather than paying to serialize every key for a full-width hash.
for type in UInt128 UInt256
do
    table="lc_integer_aggregation_04907_${type}"

    in_memory_result=$(
        $CLICKHOUSE_CLIENT "${common_options[@]}" --optimize_aggregation_in_order=0 \
            --optimize_read_in_order=0 --max_threads=1 \
            --group_by_two_level_threshold=1 --group_by_two_level_threshold_bytes=0 \
            --max_bytes_before_external_group_by=0 --max_bytes_ratio_before_external_group_by=0 -q "
                SELECT ifNull(toString(k), 'NULL'), sum(v), count()
                FROM ${table}
                GROUP BY k
                ORDER BY k NULLS FIRST")
    spilled_result=$(
        $CLICKHOUSE_CLIENT "${common_options[@]}" --optimize_aggregation_in_order=0 \
            --optimize_read_in_order=0 --max_threads=1 \
            --group_by_two_level_threshold=1 --group_by_two_level_threshold_bytes=0 \
            --max_bytes_before_external_group_by=1 --max_bytes_ratio_before_external_group_by=0 \
            --log_comment="${spill_log_prefix}${type}" -q "
                SELECT ifNull(toString(k), 'NULL'), sum(v), count()
                FROM ${table}
                GROUP BY k
                ORDER BY k NULLS FIRST")

    if [[ "$in_memory_result" != "$spilled_result" ]]
    then
        echo "In-memory and spilled aggregation results differ for ${type}" >&2
        exit 1
    fi
    printf '%s\tspill results equal\t1\n' "$type"

    nullable_in_memory_result=$(
        $CLICKHOUSE_CLIENT "${common_options[@]}" --optimize_aggregation_in_order=0 \
            --optimize_read_in_order=0 --max_threads=1 \
            --group_by_two_level_threshold=1 --group_by_two_level_threshold_bytes=0 \
            --max_bytes_before_external_group_by=0 --max_bytes_ratio_before_external_group_by=0 -q "
                SELECT ifNull(toString(k_nullable), 'NULL'), sum(v), count()
                FROM ${table}
                GROUP BY k_nullable
                ORDER BY k_nullable NULLS FIRST")
    nullable_spilled_result=$(
        $CLICKHOUSE_CLIENT "${common_options[@]}" --optimize_aggregation_in_order=0 \
            --optimize_read_in_order=0 --max_threads=1 \
            --group_by_two_level_threshold=1 --group_by_two_level_threshold_bytes=0 \
            --max_bytes_before_external_group_by=1 --max_bytes_ratio_before_external_group_by=0 \
            --log_comment="${spill_log_prefix}${type}_nullable" -q "
                SELECT ifNull(toString(k_nullable), 'NULL'), sum(v), count()
                FROM ${table}
                GROUP BY k_nullable
                ORDER BY k_nullable NULLS FIRST")

    if [[ "$nullable_in_memory_result" != "$nullable_spilled_result" ]]
    then
        echo "In-memory and spilled nullable aggregation results differ for ${type}" >&2
        exit 1
    fi
    printf '%s\tnullable spill results equal\t1\n' "$type"

    nullable_merge_method=$(
        $CLICKHOUSE_CLIENT "${common_options[@]}" --send_logs_level=trace \
            --optimize_aggregation_in_order=0 --optimize_read_in_order=0 --max_threads=1 \
            --group_by_two_level_threshold=1 --group_by_two_level_threshold_bytes=0 \
            --max_bytes_before_external_group_by=1 --max_bytes_ratio_before_external_group_by=0 \
            -q "SELECT sum(v) FROM ${table} GROUP BY k_nullable FORMAT Null" 2>&1 \
            | grep -oE 'External aggregation merge method: [a-z_0-9]+' | sort -u)
    expected_nullable_merge_method="External aggregation merge method: low_cardinality_key${type#UInt}"
    if [[ "$nullable_merge_method" != "$expected_nullable_merge_method" ]]
    then
        echo "Unexpected nullable external merge method for ${type}: ${nullable_merge_method}" >&2
        exit 1
    fi
    printf '%s\tnullable external merge method\t%s\n' "$type" "$nullable_merge_method"
done

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT -q "
    SELECT
        replaceOne(log_comment, '${log_prefix}', '') AS key_type,
        max(ProfileEvents['AggregationConvertedToTwoLevel']) > 0 AS converted
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND startsWith(log_comment, '${log_prefix}')
      AND type = 'QueryFinish'
    GROUP BY log_comment
    ORDER BY toUInt16(substring(key_type, 5))"

$CLICKHOUSE_CLIENT -q "
    SELECT
        replaceOne(log_comment, '${spill_log_prefix}', '') AS key_type,
        max(ProfileEvents['ExternalAggregationWritePart']) > 0 AS wrote_part,
        max(ProfileEvents['ExternalAggregationMerge']) > 0 AS merged
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND startsWith(log_comment, '${spill_log_prefix}')
      AND type = 'QueryFinish'
    GROUP BY log_comment
    ORDER BY key_type"

for type in "${types[@]}"
do
    $CLICKHOUSE_CLIENT -q "DROP TABLE lc_integer_aggregation_04907_${type} SYNC"
done
