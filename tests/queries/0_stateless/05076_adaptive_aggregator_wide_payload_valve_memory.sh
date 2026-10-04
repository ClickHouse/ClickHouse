#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The companion of `05059_adaptive_aggregator_pressure_valve_memory` for the case its bound could
# not see. A staged chunk of a general aggregate carries the gathered argument columns beside its
# keys, and the batch a pressure sweep claims keeps holding them until its drain finishes, so a
# bound computed from the destination table alone - the records' bookkeeping and the fixed width
# of their aggregate states, plus the staged key bytes - is blind to a stream whose aggregate
# arguments are wide and variable-width. Here they are the widest thing in the first query: the
# states are one `uniq` sketch over a single distinct value per group, while every record stages
# a couple of hundred bytes of argument, so the claim's whole cost is what the old bound did not
# count and the sweep could build a working set the threshold cannot hold.
#
# The second query is the wide-key shape, whose batches the finish drain used to size from a
# record count derived from the fixed state width alone, so a backlog of wide staged bytes
# overshot the part bound at the merge boundary.
#
# All-distinct keys keep every producer frozen: nothing repeats, so the thaw verdict cannot fire
# and the whole stream goes through the staging path, which is where the drains run. The
# thresholds are pinned per query because the runner randomizes them, and `max_memory_usage` is
# the cell that encodes the claim: ten times the threshold the query asked to spill at, which a
# valve sized from that threshold stays well inside.
#
# Each query runs in its own clickhouse-local process, so the counters in `system.events` belong
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
SET max_memory_usage = 300000000;
SET max_threads = 4;
SET max_block_size = 8192;

SELECT count(), sum(u) FROM (
    SELECT concat('an-ordinary-looking-group-key-', toString(number)) AS k,
        uniq(concat(repeat('wide-aggregate-argument-', 8), toString(number))) AS u
    FROM numbers_mt(1500000) GROUP BY k);

SELECT 'went external', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ExternalAggregationWritePart') > 0;
SELECT 'the valve ran', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureSweeps') > 0;
SELECT 'the tables froze', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;
SELECT 'the drain carried the stream', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureDrainedRecords') > 1000000;
SELECT 'stayed on the frozen path',
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws') = 0
    AND (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureStandDowns') = 0;
"

$CLICKHOUSE_LOCAL --query "
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 1000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 1000;
SET group_by_two_level_threshold_bytes = 1000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 20000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_memory_usage = 300000000;
SET max_threads = 4;
SET max_block_size = 8192;

SELECT count(), sum(u) FROM (
    SELECT concat('a-deliberately-long-group-key-that-stages-wide-', toString(number)) AS k,
        uniq(number % 7) AS u
    FROM numbers_mt(1000000) GROUP BY k);

SELECT 'went external', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ExternalAggregationWritePart') > 0;
SELECT 'a drain converted the backlog', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureDrainedRecords') > 0;
SELECT 'stayed on the frozen path',
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws') = 0
    AND (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureStandDowns') = 0;
"
