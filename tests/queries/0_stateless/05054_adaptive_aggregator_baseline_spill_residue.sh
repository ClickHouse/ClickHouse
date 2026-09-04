#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A thread back on the baseline aggregation decides its own spill from query memory, which includes
# the adaptive session's shared drain table, and flushing its own table cannot free that table. Left
# resident below the spill floor no sweep writes it either, so the threshold stays crossed on every
# following block and each block leaves a temporary file holding a single block's keys. The part
# count then tracks the block count instead of the number of pressure sweeps.
#
# Each query runs in its own clickhouse-local process, so the counters in `system.events` belong to
# that query alone.

$CLICKHOUSE_LOCAL --query "
SET max_threads = 4;
SET max_block_size = 8192;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 1000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 1000;
SET group_by_two_level_threshold_bytes = 1000000;
SET max_bytes_before_external_group_by = 80000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_memory_usage = 300000000;
-- The hash-table statistics remember the thaw verdict and a marked query skips the adaptive
-- engagement, so without this only the first run of the shape would reach it.
SET collect_hash_table_stats_during_aggregation = 0;

-- A repeat-dominated stream of wide keys: the tables freeze, the staged stream proves
-- repeat-dominated and thaws every thread back to the baseline path, and the records swept before
-- the thaw stay in the shared table, which holds fewer keys than the spill floor. The residue is
-- the dominant resident term through the count and the width of its keys, so releasing it is what
-- brings query memory back under the threshold.
SELECT count() FROM
(
    SELECT concat(toString(number % 400000), repeat('x', 60)) AS k
    FROM numbers_mt(6000000)
    GROUP BY k
);

-- The sweep and thaw counters are asserted next to the part count, so the test cannot pass by
-- never engaging the adaptive aggregator or never reaching memory pressure at all. The parts are
-- bounded by the sweeps rather than by a constant so that the bound holds at any scale: one part
-- per sweep is the intended rate, one part per block is the defect.
SELECT 'residue released', sumIf(value, event = 'AdaptiveAggregationResidueReleases') > 0 FROM system.events;
SELECT 'parts track sweeps not blocks',
       sumIf(value, event = 'ExternalAggregationWritePart') > 0
       AND sumIf(value, event = 'ExternalAggregationWritePart')
           <= sumIf(value, event = 'AdaptiveAggregationPressureSweeps')
FROM system.events;
SELECT 'swept under pressure', coalesce(max(value), 0) > 0 FROM system.events WHERE event = 'AdaptiveAggregationPressureSweeps';
SELECT 'thawed', coalesce(max(value), 0) > 0 FROM system.events WHERE event = 'AdaptiveAggregationThaws';
"

# A session in which no thread ever froze has no shared drain table at all, while a learning thread
# still reaches the baseline path on its own by giving up on freezing. The hot prefix holds every
# thread below the freeze threshold in keys and the unique tail carries query memory over the
# external threshold, so the baseline spill decision runs with the shared table absent.
$CLICKHOUSE_LOCAL --query "
SET max_threads = 4;
SET max_block_size = 8192;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 1000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 1000;
SET group_by_two_level_threshold_bytes = 1000000;
SET max_bytes_before_external_group_by = 80000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET collect_hash_table_stats_during_aggregation = 0;

SELECT count() FROM
(
    SELECT if(number < 1500000, 'hot', concat(toString(number), repeat('x', 60))) AS k
    FROM numbers_mt(3000000)
    GROUP BY k
);

-- Going external is what proves the baseline spill decision was actually taken: parts are written
-- only once query memory crosses the external threshold, which is the condition guarding the
-- release. Without it the scenario could pass green having never reached the code it covers.
SELECT 'went external', sumIf(value, event = 'ExternalAggregationWritePart') > 0 FROM system.events;
SELECT 'gave up without freezing', coalesce(max(value), 0) > 0 FROM system.events WHERE event = 'AdaptiveAggregationGiveUps';
SELECT 'no shared table was created', coalesce(max(value), 0) = 0 FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes';
"

# A residue exists and the baseline producers are over the external threshold, but no table is
# two-level and none is worth converting, so the baseline aggregation writes nothing at all here.
# The release has to follow it: writing the shared table on a block that was never going to spill
# would put the whole aggregation on the external merge path instead of the in-memory one.
$CLICKHOUSE_LOCAL --query "
SET max_threads = 4;
SET max_block_size = 8192;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 1000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
-- Thresholds high enough that no producer table ever reaches either one, so none is two-level and
-- none is worth converting. They must still be non-zero: two-level aggregation being disabled
-- outright turns the adaptive aggregator off, and then there is no shared table to leave alone.
SET group_by_two_level_threshold = 100000000;
SET group_by_two_level_threshold_bytes = 100000000000;
SET max_bytes_before_external_group_by = 80000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET collect_hash_table_stats_during_aggregation = 0;

SELECT count() FROM
(
    SELECT concat(toString(number % 400000), repeat('x', 60)) AS k
    FROM numbers_mt(6000000)
    GROUP BY k
);

SELECT 'nothing written at all', sumIf(value, event = 'ExternalAggregationWritePart') = 0 FROM system.events;
SELECT 'residue was there to leave alone', sumIf(value, event = 'AdaptiveAggregationPressureSweeps') > 0 FROM system.events;
SELECT 'thawed onto the baseline path', sumIf(value, event = 'AdaptiveAggregationThaws') > 0 FROM system.events;
"

# The scenarios above aggregate keys only, so no aggregate state travels through the release. A lone
# count() is staged as an inline counter rather than as an aggregate state, so this scenario carries a
# second aggregate: with two of them the records stage real states, and both exact totals move if a
# state contribution is lost or corrupted. The memory limit is deliberately not pinned here - live
# states raise the spilled volume, and this scenario checks results, not the limit.
$CLICKHOUSE_LOCAL --query "
SET max_threads = 4;
SET max_block_size = 8192;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 1000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 1000;
SET group_by_two_level_threshold_bytes = 1000000;
SET max_bytes_before_external_group_by = 80000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET collect_hash_table_stats_during_aggregation = 0;

SELECT count(), sum(s), sum(c) FROM
(
    SELECT concat(toString(number % 400000), repeat('x', 60)) AS k, sum(number) AS s, count() AS c
    FROM numbers_mt(6000000)
    GROUP BY k
);

SELECT 'released with live states', sumIf(value, event = 'AdaptiveAggregationResidueReleases') > 0 FROM system.events;
SELECT 'thawed onto the baseline path', sumIf(value, event = 'AdaptiveAggregationThaws') > 0 FROM system.events;
"
