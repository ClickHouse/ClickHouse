#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The adaptive aggregator's pressure sweeps are the memory valve of the frozen path: they claim
# a batch of the staged backlogs, drain it into a hash table and write that table out through
# the ordinary external aggregation, so that the query stays at
# `max_bytes_before_external_group_by`. The claim, the residue the sub-part tails share and the
# budget for the tables in flight to the writer used to be bounded by absolute constants - a
# million keys and 256 MiB - which know nothing of the threshold, so the valve's own working set
# could be tens of times the budget it was defending and the query died on `max_memory_usage`
# while it was supposed to be spilling.
#
# All-distinct keys keep every producer frozen: nothing repeats, so the thaw verdict cannot fire
# and the whole stream goes through the staging path, which is where the valve runs. The
# thresholds are pinned per query because the runner randomizes them, and `max_memory_usage` is
# the cell that encodes the claim - ten times the threshold the query asked to spill at, which a
# valve sized from that threshold stays well inside, and which the absolute constants blow past.
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
SET max_memory_usage = 300000000;
SET max_threads = 4;
SET max_block_size = 8192;

SELECT count(), sum(u) FROM (
    SELECT concat('an-ordinary-looking-group-key-', toString(number)) AS k, uniq(number % 11) AS u
    FROM numbers_mt(3000000) GROUP BY k);

-- Without these the limit above could be met by not spilling at all, by never engaging the
-- aggregator, or by leaving the frozen path for the baseline one, which is not the path the
-- claim is about.
SELECT 'went external', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ExternalAggregationWritePart') > 0;
SELECT 'the valve ran', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureSweeps') > 0;
SELECT 'the tables froze', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;
SELECT 'the drain carried the stream', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureDrainedRecords') > 2000000;
SELECT 'stayed on the frozen path',
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws') = 0
    AND (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureStandDowns') = 0;
"
