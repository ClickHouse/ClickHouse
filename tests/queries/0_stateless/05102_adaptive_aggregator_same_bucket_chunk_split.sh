#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# The companion of `05101_adaptive_aggregator_skewed_payload_chunk_split` for a staged chunk
# whose bytes sit in a few records that all route to one two-level bucket. A chunk over half a
# pressure part is cut at publication into pieces the bound admits, along bucket boundaries where
# the buckets fit; a bucket that is over the bound on its own used to go out as one piece, which
# the next claim took whole and drained into a table far over the part the bound exists to keep -
# the very case the cut is for, reached through a single bucket instead of a single chunk. Such a
# bucket is now cut inside, record by record, so only a single record over the bound can still come
# out whole.
#
# The shape puts the chunk's bytes into sixty-four records with a one-megabyte argument each,
# among sixty-five thousand records with an empty one. The two-level bucket of a `UInt64` key is
# the top byte of its CRC-32C, and these sixty-four keys are the first sixty-four whose top byte
# is 120: all sixty-four megabytes land in one bucket, six times over the ten-megabyte bound, so
# no cut along bucket boundaries can bring the chunk under it. The heavy rows sit in a later
# block, because a producer's first block goes into its own table before it freezes and only the
# blocks after that are staged; all keys are distinct, so nothing repeats, the thaw verdict cannot
# fire and the whole stream goes through the staging path. The threshold is pinned because the
# runner randomizes it.
#
# The query runs in its own clickhouse-local process, so the counters in `system.events` belong
# to it alone. The memory limit is a loose ceiling: the cut itself holds the block, the chunk and
# the pieces at once, three times the chunk's bytes, whichever way the pieces are cut, so the
# assertions that carry this test are that the chunk was cut and that no piece came out over the
# bound.
$CLICKHOUSE_LOCAL --query "
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 1000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 1000;
SET group_by_two_level_threshold_bytes = 1000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 20000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_memory_usage = 500000000;
SET max_threads = 2;
SET max_block_size = 65536;

WITH number BETWEEN 400000 AND 400063 AS heavy
SELECT count(), sum(length(m)) FROM (
    SELECT
        if(heavy, arrayElement([9,268,515,774,1053,1304,1559,1810,2131,2390,2649,2908,3143,3394,3661,3912,4285,4536,4791,5042,5289,5548,5795,6054,6375,6626,6893,7144,7411,7670,7929,8188,8292,8545,8814,9067,9328,9589,9850,10111,10302,10555,10804,11057,11306,11567,11808,12069,12496,12757,13018,13279,13508,13761,14030,14283,14474,14735,14976,15237,15518,15771,16020,16273]::Array(UInt64), toUInt32(number - 400000 + 1)), number + 10000000) AS k,
        max(repeat('x', if(heavy, 1000000, 0))) AS m
    FROM numbers_mt(1500000) GROUP BY k);

SELECT 'went external', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ExternalAggregationWritePart') > 0;
SELECT 'the valve ran', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureSweeps') > 0;
SELECT 'the tables froze', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;
SELECT 'the same-bucket chunk was cut', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedChunkSplits') > 0;
SELECT 'no piece came out over the bound', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedChunkPiecesOverBound') = 0;
SELECT 'stayed on the frozen path',
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws') = 0
    AND (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureStandDowns') = 0;
"
