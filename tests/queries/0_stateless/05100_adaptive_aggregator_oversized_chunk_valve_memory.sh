#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The companion of `05059_adaptive_aggregator_pressure_valve_memory` for a staged chunk that is
# alone over the part bound. The pressure sweeps claim whole chunks and stop between them, so the
# bound on a claim holds only at the granularity of a chunk: a chunk whose drain alone is over
# the bound used to be claimed whole, and its drain built the over-budget table the bound exists
# to prevent. `uniqUpTo(100)` has a fixed state of about 800 bytes, some thirty times the
# 24 bytes its record occupies staged, so a sealed chunk of a few megabytes drains into a table
# of hundreds of megabytes - many times the threshold it was supposed to hold. Such a chunk is
# now cut at publication, along bucket boundaries, into pieces of no more than half a part, which
# the claims then take a part at a time, and a sweep keeps claiming while the query is over the
# threshold, so a block that published more than a part does not leave the difference behind.
#
# All-distinct keys keep every producer frozen: nothing repeats, so the thaw verdict cannot fire
# and the whole stream goes through the staging path, which is where the valve runs. The
# threshold is pinned because the runner randomizes it, and `max_memory_usage` is the cell that
# encodes the claim: ten times the threshold the query asked to spill at, which the drain of one
# uncut chunk overshot on its own.
#
# The query runs in its own clickhouse-local process, so the counters in `system.events` belong
# to it alone.
$CLICKHOUSE_LOCAL --query "
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 1000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 1000;
SET group_by_two_level_threshold_bytes = 1000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 20000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_memory_usage = 200000000;
SET max_threads = 2;
SET max_block_size = 65536;

SELECT count(), sum(u) FROM (SELECT number AS k, uniqUpTo(100)(number) AS u FROM numbers_mt(1200000) GROUP BY k);

SELECT 'went external', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ExternalAggregationWritePart') > 0;
SELECT 'the valve ran', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureSweeps') > 0;
SELECT 'the tables froze', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;
SELECT 'oversized chunks were cut', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedChunkSplits') > 0;
SELECT 'the drain carried the stream', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureDrainedRecords') > 0;
SELECT 'stayed on the frozen path',
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws') = 0
    AND (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureStandDowns') = 0;
"
