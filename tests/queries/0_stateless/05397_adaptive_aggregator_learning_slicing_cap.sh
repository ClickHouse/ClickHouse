#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A learning table of the adaptive aggregator slices its blocks to freeze exactly at the key-count threshold until it
# has taken more than 16 times the threshold in rows; later blocks are aggregated whole, and the check between blocks
# still freezes the table. Each of the two streams holds 10 keys for its first 60 blocks and new keys after them, so
# the tables pass that cap long before they grow. Each table then takes its whole first block of new keys and freezes
# at the block boundary, so all the new-key rows but those two blocks are staged; a freeze inside the block would
# leave part of each of those blocks staged as well. The result matches the same query with the feature off.
# `max_threads_min_free_memory_per_thread = 0` keeps both threads whatever the free memory, so each stream has a
# table of its own. The thresholds are pinned because the runner randomizes them, and the query runs in its own
# clickhouse-local process, so the counters in `system.events` belong to it alone.
query="SELECT count(), sum(c) FROM (
    SELECT if(number < 491520, number % 10, number) AS k, count() AS c
    FROM (SELECT number FROM numbers(983040) UNION ALL SELECT number FROM numbers(983040))
    GROUP BY k SETTINGS __SETTINGS__)"
off=${query//__SETTINGS__/enable_adaptive_aggregator = 0}
on=${query//__SETTINGS__/enable_adaptive_aggregator = 1}

$CLICKHOUSE_LOCAL --query "
SET max_threads = 2, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
SET adaptive_aggregator_freeze_threshold = 1000, adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 0;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT 'matches the baseline', (${off}) = (${on});
SELECT 'tables froze at a block boundary',
    (SELECT sum(value) FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords') = 2 * 491520 - 2 * 8192;
"
