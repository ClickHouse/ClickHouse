#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# The valve sizes the batch a pressure sweep claims from the records' bookkeeping, the fixed width
# of their aggregate states and the bytes the chunks stage, and it reads the built table back
# through `allocatedBytes`, which sums arenas and hash-table buffers. States that own memory outside
# the arenas - the hash set of `uniqExact`, the bitmap of `groupBitmap` - are seen by neither
# reading, so this is the shape that would let a batch build a working set the threshold cannot
# hold if their heap were unbounded relative to what is counted. It is not: a state's heap grows
# with the values it absorbed, which the batch staged as argument bytes, and its per-group floor
# is a small multiple of the per-record bookkeeping. The sweeps also account the tables they
# build by the drain's own tracked allocation, which does see that heap: a producer-local table
# corrects its reservation against the detached-bytes budget to it, and the shared table, which
# the tail drains grow across many sweeps, reaches the part bound by it.
#
# Every key repeats four times in a row, so each group's states hold several values and the stream
# is still not repeat-dominated: nothing thaws, and every record goes through the staging path
# where the drains run. Most sweeps find a backlog smaller than a part and drain it into the shared
# table, so the shape covers that table's path to disk as well as the producer-local one: it is
# asserted to have been written out at the part bound. `groupArray` rides along as the arena-owning
# kind for comparison, and the three exact totals move if a state contribution is lost across the
# spill. The thresholds are pinned because the runner randomizes them, and `max_memory_usage` is the
# cell that encodes the claim: ten times the threshold the query asked to spill at.
#
# The query runs in its own clickhouse-local process, so the counters in `system.events` belong to
# it alone.
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
SET max_threads = 4;
SET max_block_size = 8192;

SELECT count(), sum(u), sum(b), sum(a) FROM (
    SELECT concat('an-ordinary-looking-group-key-', toString(intDiv(number, 4))) AS k,
        uniqExact(number) AS u,
        bitmapCardinality(groupBitmapState(number)) AS b,
        length(groupArray(number)) AS a
    FROM numbers_mt(1000000) GROUP BY k);

SELECT 'went external', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ExternalAggregationWritePart') > 0;
SELECT 'the valve ran', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureSweeps') > 0;
SELECT 'the tables froze', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;
SELECT 'the drain carried the stream', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureDrainedRecords') > 500000;
SELECT 'the shared table was written at the part bound', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationSharedTableSpills') > 0;
SELECT 'stayed on the frozen path',
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws') = 0
    AND (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureStandDowns') = 0;
"
