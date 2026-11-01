#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# Repeated keys make staging more expensive than updating ordinary states. The cells cover wide
# arguments and the record formats that carry only keys or count multiplicities. Each cell checks
# the result, the first execution's thaw decision, and the stored verdict that keeps the second
# execution on the ordinary path. Wide arguments cause thawing during the first execution. Count
# and key-only records cross the end-of-input admission bound without crossing the stricter thaw bound.
input="SELECT number FROM numbers(0, 200000)
    UNION ALL SELECT number FROM numbers(200000, 200000)
    UNION ALL SELECT number FROM numbers(400000, 200000)
    UNION ALL SELECT number FROM numbers(600000, 200000)"
settings="SET max_threads = 4, max_block_size = 4096;
    SET optimize_injective_functions_in_group_by = 0;
    SET adaptive_aggregator_freeze_threshold = 32, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000;
    SET adaptive_aggregator_disable_thaw = 0, collect_hash_table_stats_during_aggregation = 1;
    SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;"
counters="SELECT sumIf(value, event = 'AdaptiveAggregationLocalFreezes') AS freezes,
    sumIf(value, event = 'AdaptiveAggregationThaws') AS thaws FROM system.events"

check()
{
    local label=$1 key=$2 aggregates=$3 expect_thaw=$4
    local query="SELECT ${key} AS k${aggregates:+, ${aggregates}} FROM (${input}) GROUP BY k"
    for adaptive in 0 1
    do
        $CLICKHOUSE_LOCAL --query "
            ${settings}
            SET enable_adaptive_aggregator = ${adaptive};
            SELECT sum(cityHash64(*)) FROM (${query});
            CREATE TEMPORARY TABLE after_first ENGINE = Memory AS ${counters};
            SELECT sum(cityHash64(*)) FROM (${query});
            SELECT '${label}',
                (SELECT freezes > 0 AND (thaws > 0) = ${expect_thaw} FROM after_first),
                freezes = (SELECT freezes FROM after_first) AND thaws = (SELECT thaws FROM after_first)
                FROM (${counters});
        " > "${CLICKHOUSE_TMP}/fixed_payload_cost_${adaptive}.out"
    done
    diff -u <(head -n 2 "${CLICKHOUSE_TMP}/fixed_payload_cost_0.out") \
        <(head -n 2 "${CLICKHOUSE_TMP}/fixed_payload_cost_1.out")
    tail -n 1 "${CLICKHOUSE_TMP}/fixed_payload_cost_1.out"
}

# The same wide column is used by two aggregates and occupies only one field in the staged record.
check 'Wide numeric argument' 'toUInt32(number % 20000)' 'sum(toUInt256(number)), min(toUInt256(number))' 1
check 'Nullable fixed argument' 'toUInt32(number % 20000)' \
    'min(if(number % 3 = 0, NULL, toFixedString(toString(number % 97), 32)))' 1
check 'Mixed fixed and variable arguments' 'toString(number % 20000)' \
    'sum(toUInt256(number)), min(toString(number % 8))' 1
check 'Count records' 'toUInt32(number % 20000)' 'count(number)' 0
check 'Keys without aggregates' 'toUInt32(number % 20000)' '' 0
