#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# The drains claim staged chunks in order up to the pressure part bound. A claim used to take the
# chunk it crossed the bound on and stop after it, so two chunks each just under the bound were
# drained together into one table of twice the part the bound exists to keep. The chunks of that
# shape are the pieces a cut chunk publishes when a single record is over half a part on its own:
# such a record cannot be cut further and goes out as a piece of its own, over half a part but
# under a whole one, and two of them in one backlog were claimed together. A claim now closes
# before the chunk that would take it to the bound, and only a first chunk that is over the bound
# alone is taken regardless.
#
# The shape stages three records with a twenty-megabyte argument each (`repeat` caps its count at
# a million, so a one-megabyte string is repeated twenty times), in one block among sixty-five
# thousand records with an empty one, at a threshold whose part bound is the thirty-two-megabyte
# floor: the chunk of sixty megabytes is cut at half a part, the heavy records come out as three
# pieces each over that half and under the whole, and a claim that takes one of them must leave
# the next to another claim. The heavy keys are the first three whose two-level bucket - the top
# byte of the CRC-32C of a `UInt64` key - is 120, and the rows are consecutive, so the pieces are
# published back to back: a sweep of the other thread could otherwise slip in between two pieces
# and take one alone, and with three pieces it would have to do so twice. The heavy rows sit in a
# later block, because a producer's first block goes into its own table before it freezes and only
# the blocks after that are staged; all keys are distinct, so the thaw verdict cannot fire and the
# whole stream goes through the staging path. The threshold is pinned because the runner
# randomizes it.
#
# The query runs in its own clickhouse-local process, so the counters in `system.events` belong
# to it alone. The memory limit is a loose ceiling: the cut itself holds the block, the chunk and
# the pieces at once, so the assertions that carry this test are the three pieces over the bound
# and a claim closed before one of them.
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

WITH number BETWEEN 400000 AND 400002 AS heavy
SELECT count(), sum(length(m)) FROM (
    SELECT
        if(heavy, arrayElement([9, 268, 515]::Array(UInt64), toUInt32(number - 400000 + 1)), number + 10000000) AS k,
        max(repeat(repeat('x', 1000000), if(heavy, 20, 0))) AS m
    FROM numbers_mt(1500000) GROUP BY k);

SELECT 'went external', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ExternalAggregationWritePart') > 0;
SELECT 'the valve ran', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureSweeps') > 0;
SELECT 'the tables froze', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;
SELECT 'the chunk was cut', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedChunkSplits') > 0;
SELECT 'three single records came out over half a part', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedChunkPiecesOverBound') = 3;
SELECT 'a claim closed before one of them', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedClaimsClosedAtBound') > 0;
SELECT 'stayed on the frozen path',
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws') = 0
    AND (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureStandDowns') = 0;
"
