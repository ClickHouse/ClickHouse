#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# Four independent streams keep the amount of repetition per producer stable across thread schedules.
input="SELECT number AS n FROM numbers(0, 800000)
    UNION ALL SELECT number AS n FROM numbers(800000, 800000)
    UNION ALL SELECT number AS n FROM numbers(1600000, 800000)
    UNION ALL SELECT number AS n FROM numbers(2400000, 800000)"
settings="SET max_threads = 4, max_block_size = 8192;
    SET adaptive_aggregator_freeze_threshold = 8192, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000;
    SET adaptive_aggregator_disable_thaw = 0, collect_hash_table_stats_during_aggregation = 1;
    SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;"

# Each group receives two distinct values while its key repeats across blocks. The numeric and string
# arguments exercise both hash widths, and tuple and variadic arguments use the generic implementation
# of `uniqCombined`. Each query runs twice so the result comparison covers recording the admission
# verdict and reusing it on the next execution.
for aggregate in \
    'uniqCombined(16)(n % 200000)' \
    'uniqCombined64(16)(n % 200000)' \
    'uniqCombined(16)(toString(n % 200000))' \
    'uniqCombined(16)(tuple(n % 200000, toString(n % 200000)))' \
    'uniqCombined(16)(n % 200000, toString(n % 200000))'
do
    query="SELECT sum(cityHash64(*)) FROM
        (SELECT n % 100000 AS k, ${aggregate} AS u
         FROM (${input}) GROUP BY k)"
    for adaptive in 0 1
    do
        $CLICKHOUSE_LOCAL --query "
            ${settings}
            SET enable_adaptive_aggregator = ${adaptive};
            ${query}; ${query};
        " > "${CLICKHOUSE_TMP}/retained_state_${adaptive}.out"
    done
    diff -u "${CLICKHOUSE_TMP}/retained_state_0.out" "${CLICKHOUSE_TMP}/retained_state_1.out"
    echo "$aggregate"
done

# Narrow staged arguments remain cheaper than the local tables' hash buffers and aggregate states.
# The stored verdict must allow the second execution to freeze its tables as well.
query="SELECT n % 100000 AS k, uniqCombined(16)(n % 200000) FROM (${input}) GROUP BY k"
$CLICKHOUSE_LOCAL --query "
    ${settings}
    SET enable_adaptive_aggregator = 1;
    ${query} FORMAT Null;
    CREATE TEMPORARY TABLE freezes_after_first ENGINE = Memory AS
        SELECT coalesce(sum(value), 0) AS freezes FROM system.events
        WHERE event = 'AdaptiveAggregationLocalFreezes';
    ${query} FORMAT Null;
    SELECT 'Repeated admission', (SELECT freezes FROM freezes_after_first) > 0,
        coalesce(sum(value), 0) > (SELECT freezes FROM freezes_after_first)
        FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes';
"
