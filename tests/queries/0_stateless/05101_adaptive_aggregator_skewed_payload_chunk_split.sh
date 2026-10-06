#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# The companion of `05100_adaptive_aggregator_oversized_chunk_valve_memory` for a staged chunk
# whose bytes sit in a few of its records. A chunk over half a pressure part is cut at publication
# along bucket boundaries into pieces the bound admits, and the pieces are sized from their
# records: the routing hashes, the key bytes and the aggregate arguments. The argument columns
# used to be prorated by the record count, which is right when the buckets share the bytes evenly,
# as keys routed by hash do, and wrong when a few records hold most of them and share a bucket
# range: such a range was measured at the average and came out as one piece far over the bound,
# which the next claim took whole - the case the cut exists to prevent. The arguments are now
# summed record by record, so a piece is over the bound only when a single bucket is.
#
# The shape puts the chunk's bytes into sixty-four records with a one-megabyte argument each,
# among sixty-five thousand records with an empty one. The two-level bucket of a `UInt64` key is
# the top byte of its CRC-32C, and these sixty-four keys were chosen so that their top four bits
# agree: they route to the sixteen consecutive buckets 112 to 127, four megabytes a bucket, so no
# bucket is over the bound on its own and the whole run of them is four times over it. Prorated,
# the sixty-four megabytes are averaged over all two hundred and fifty-six buckets and one range
# of some sixty buckets holds most of the run; summed, a piece holds at most four of the heavy
# buckets. The heavy rows sit in a later block, because a producer's first block goes into its
# own table before it freezes and only the blocks after that are staged; all keys are distinct, so
# nothing repeats, the thaw verdict cannot fire and the whole stream goes through the staging
# path. The threshold is pinned because the runner randomizes it.
#
# The query runs in its own clickhouse-local process, so the counters in `system.events` belong
# to it alone. The memory limit is a loose ceiling: the cut itself holds the block, the chunk and
# the pieces at once, three times the chunk's bytes, whichever way the pieces are sized, so the
# assertion that carries this test is the count of pieces over the bound.
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
        if(heavy, arrayElement([9, 32, 41, 82, 91, 114, 123, 150, 159, 182, 191, 196, 205, 228, 237, 261, 268, 293, 300, 343, 350, 375, 382, 403, 410, 435, 442, 449, 456, 481, 488, 515, 522, 547, 554, 593, 600, 625, 632, 661, 668, 693, 700, 711, 718, 743, 750, 774, 783, 806, 815, 852, 861, 884, 893, 912, 921, 944, 953, 962, 971, 994, 1003, 1044]::Array(UInt64), toUInt32(number - 400000 + 1)), number + 10000000) AS k,
        max(repeat('x', if(heavy, 1000000, 0))) AS m
    FROM numbers_mt(1500000) GROUP BY k);

SELECT 'went external', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ExternalAggregationWritePart') > 0;
SELECT 'the valve ran', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureSweeps') > 0;
SELECT 'the tables froze', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;
SELECT 'the skewed chunk was cut', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedChunkSplits') > 0;
SELECT 'no piece came out over the bound', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedChunkPiecesOverBound') = 0;
SELECT 'stayed on the frozen path',
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws') = 0
    AND (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureStandDowns') = 0;
"
