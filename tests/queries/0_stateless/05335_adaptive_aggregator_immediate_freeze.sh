#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A zero key-count threshold freezes each producer before its first key is inserted. The empty frozen tables contribute
# no measured per-key cost, and all input is staged for the merge. A positive threshold retains keys and measures their
# allocation cost before freezing. The keys are `UInt64`, whose method has a two-level form, so the adaptive aggregator
# engages, and every cell says whether the tables froze; the tables do not thaw, so the staged input stays staged.
# `max_threads_min_free_memory_per_thread = 0` keeps the threads of every cell whatever the free memory. Each cell runs
# in its own clickhouse-local process, so the counters in `system.events` belong to it alone.
function run_cell()
{
    local label=$1
    local threshold=$2
    local query=$3
    echo "${label}"
    $CLICKHOUSE_LOCAL --query "
    SET enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = ${threshold}, adaptive_aggregator_disable_thaw = 1;
    SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
    SET group_by_two_level_threshold = 10000, group_by_two_level_threshold_bytes = 5000000;
    SET collect_hash_table_stats_during_aggregation = 0;
    SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

    ${query};
    SELECT 'frozen', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;
    "
}

run_cell 'Immediate freeze with inline count' 0 \
    "SELECT count(), sum(c), sum(k) FROM (SELECT number AS k, count() AS c FROM numbers_mt(1000000) GROUP BY k)"

run_cell 'Immediate freeze with sum' 0 \
    "SELECT count(), sum(sm) FROM (SELECT toUInt64(number % 10000) AS k, sum(number) AS sm FROM numbers_mt(1000000) GROUP BY k)"

run_cell 'Immediate freeze with growing distinct states' 0 \
    "SELECT count(), sum(u) FROM (SELECT toUInt64(number % 10000) AS k, uniqExact(number) AS u FROM numbers_mt(1000000) GROUP BY k)"

run_cell 'Immediate freeze without aggregate states' 0 \
    "SELECT count(), sum(k) FROM (SELECT number AS k FROM numbers_mt(1000000) GROUP BY k)"

run_cell 'Immediate freeze with fixed distinct states' 0 \
    "SELECT
        (SELECT sum(u) FROM (SELECT toUInt64(number % 10000) AS k, uniqHLL12(number) AS u FROM numbers_mt(1000000) GROUP BY k
            SETTINGS enable_adaptive_aggregator = 0))
        =
        (SELECT sum(u) FROM (SELECT toUInt64(number % 10000) AS k, uniqHLL12(number) AS u FROM numbers_mt(1000000) GROUP BY k))"

run_cell 'Freeze with retained keys' 1 \
    "SELECT count(), sum(sm) FROM (SELECT number AS k, sum(number) AS sm FROM numbers_mt(1000000) GROUP BY k)"
