#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# Cutting each key and payload representation must preserve every key and its associated value.
# Large blocks cross the pressure bound with fixed-width keys; wide strings cross it with fewer rows.
for key_kind in fixed string; do
    if [[ "$key_kind" == fixed ]]; then
        key_expression="number"
        key_value="k"
        rows=1200000
        # Keep at least two full source blocks so aggregation retains multiple producer streams.
        block_size=524288
    else
        key_expression="concat(repeat('x', 512), toString(number))"
        key_value="toUInt64(substring(k, 513))"
        rows=150000
        block_size=65536
    fi

    for payload in count general; do
        if [[ "$payload" == count ]]; then
            aggregate_expression="count()"
            expected_sum="$rows"
            expected_value="1"
        else
            aggregate_expression="max(number)"
            expected_sum="$((rows * (rows - 1) / 2))"
            expected_value="$key_value"
        fi

        # Preserve the aggregate even when its argument is also the grouping key.
        $CLICKHOUSE_LOCAL --query "
            SET enable_adaptive_aggregator = 1;
            SET optimize_aggregators_of_group_by_keys = 0;
            SET adaptive_aggregator_freeze_threshold = 1000;
            SET adaptive_aggregator_freeze_threshold_bytes = 0;
            SET collect_hash_table_stats_during_aggregation = 0;
            SET max_bytes_before_external_group_by = 20000000;
            SET max_bytes_ratio_before_external_group_by = 0;
            SET max_memory_usage = 1000000000;
            SET max_threads = 2;
            SET max_block_size = $block_size;

            SELECT '$key_kind $payload',
                count() = $rows,
                sum(v) = $expected_sum,
                countIf(v != $expected_value) = 0,
                sum(cityHash64(k)) = (SELECT sum(cityHash64($key_expression)) FROM numbers($rows))
            FROM
            (
                SELECT $key_expression AS k, $aggregate_expression AS v
                FROM numbers_mt($rows) GROUP BY k
            );

            SELECT 'chunks split', sum(value) > 0
            FROM system.events WHERE event = 'AdaptiveAggregationStagedChunkSplits';
        "
    done
done
