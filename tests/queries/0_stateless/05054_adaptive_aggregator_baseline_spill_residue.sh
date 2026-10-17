#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A thread back on the baseline aggregation decides its own spill from query memory, which includes
# the adaptive session's shared drain table, and flushing its own table cannot free that table. Left
# resident below the part bound no sweep writes it either, so the threshold stays crossed on every
# following block and each block leaves a temporary file holding a single block's keys.
#
# The sweeps detach the shared table once it reaches the part bound, which is a key count and a byte
# bound derived from `max_bytes_before_external_group_by` - an eighth of it, never below 32 MiB. The
# scenarios that need the residue resident therefore keep it well under that bound, and the bound is
# read against the table's real footprint, not its key bytes: the drain table is two-level, so it
# carries a hash table and an arena per bucket, and measured with this binary a table of these
# 70-byte keys crossed 32 MiB at about 74000 keys. The residue holds every distinct key of the
# stream, so the streams here have 50000 distinct keys, which keeps the residue near half the bound
# with the states of the last scenario included. A threshold of 20 MB keeps every sweep in the tail
# regime and is crossed while the stream is still staged - about 35 MB of staged records precede the
# thaw - so the sweeps run before the thaw whatever the timing, and the tables the thawed threads
# build afterwards cross it again on their own. A low threshold also means many small parts, and the
# merge holds a reader per part, so the row counts are kept low enough for the merge itself to stay
# well inside the memory limit.
#
# The scenarios spill on every one of their rows, and a sanitizer build pays several times over for
# each part written and read back, so every scenario processes only as many rows as its assertions
# need: the flaky check runs the test on every core of a runner at once and fails a run that takes
# longer than its time limit, whichever copy was slowed down by the others.
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
SET max_bytes_before_external_group_by = 20000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_memory_usage = 300000000;
-- The hash-table statistics remember the thaw verdict and a marked query skips the adaptive
-- engagement, so without this only the first run of the shape would reach it.
SET collect_hash_table_stats_during_aggregation = 0;

-- A repeat-dominated stream of wide keys: the tables freeze, the staged stream proves
-- repeat-dominated and thaws every thread back to the baseline path, and the records swept before
-- the thaw stay in the shared table, which holds fewer keys and bytes than the part bound. The
-- residue is a resident term the thawed threads cannot free by flushing their own tables, so
-- releasing it is part of what brings query memory back under the threshold.
SELECT count() FROM
(
    SELECT concat(toString(number % 50000), repeat('x', 60)) AS k
    FROM numbers_mt(1000000)
    GROUP BY k
);

-- The sweep and thaw counters are asserted next to the release, so the test cannot pass by never
-- engaging the adaptive aggregator or never reaching memory pressure at all. The parts are bounded
-- against the block count - 1000000 rows in blocks of 8192 - because one part per block is the
-- defect: a thread that finds the residue resident on every block writes its own few keys out on
-- every block. A released residue lets a table grow over several blocks before it is written, and
-- the sweeps' own parts hold hundreds of thousands of records each.
SELECT 'residue released', sumIf(value, event = 'AdaptiveAggregationResidueReleases') > 0 FROM system.events;
SELECT 'parts stay far below the block count',
       sumIf(value, event = 'ExternalAggregationWritePart') > 0
       AND sumIf(value, event = 'ExternalAggregationWritePart') * 2 < intDiv(1000000, 8192)
FROM system.events;
SELECT 'swept under pressure', coalesce(max(value), 0) > 0 FROM system.events WHERE event = 'AdaptiveAggregationPressureSweeps';
SELECT 'thawed', coalesce(max(value), 0) > 0 FROM system.events WHERE event = 'AdaptiveAggregationThaws';
"

# A session in which no thread ever froze has no shared drain table at all, while a learning thread
# still reaches the baseline path on its own by giving up on freezing. A thread gives up once it
# has seen sixteen times the freeze threshold in rows while holding fewer keys than the threshold,
# and it freezes as soon as a block leaves it at the threshold or above, so the stream must keep
# every thread under a thousand keys through its first sixteen thousand rows whatever blocks it
# happens to receive. A hot prefix followed by a unique tail cannot promise that: the blocks are
# handed out on demand, and a thread that starts late on a loaded machine draws its first block
# from the tail and freezes on it. Every block here has the same mix instead - one row in
# thirty-two carries a unique key, the rest the hot one - so a thread holds about five hundred
# keys when it gives up and could not reach a thousand before then, in any order of the blocks.
#
# A learning thread also stands down without giving up when query memory is over the external
# threshold at the end of one of its blocks, and that reading is query-wide. Through the first two
# blocks of every thread the query holds a few small tables and the blocks in flight, measured at
# under 7 MB with this binary, so the threshold sits at 15 MB: it cannot be crossed until some
# thread has grown past its own give-up. The unique keys are wide so that the tables the threads
# build afterwards carry the query well over the threshold - near 30 MB at the end - and the parts
# that follow are as small as the blocks: what the scenario asserts is the decision, not their size.
$CLICKHOUSE_LOCAL --query "
SET max_threads = 4;
SET max_block_size = 8192;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 1000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 1000;
SET group_by_two_level_threshold_bytes = 1000000;
SET max_bytes_before_external_group_by = 15000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET collect_hash_table_stats_during_aggregation = 0;

SELECT count() FROM
(
    SELECT if(number % 32 = 0, concat(toString(number), repeat('x', 400)), 'hot') AS k
    FROM numbers_mt(2000000)
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
# two-level and none is worth converting, so the baseline aggregation never takes the spill decision
# here. The release has to follow that decision: writing the shared table on a block that was never
# going to spill would be a write the threshold did not ask for. The pressure sweeps may still write
# parts of their own at the part bound - that is the valve doing its job - so what is asserted is the
# release counter, not the absence of parts. The shape is the first scenario's, whose residue and
# producer tables together are what carry query memory over the threshold.
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
SET max_bytes_before_external_group_by = 20000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET collect_hash_table_stats_during_aggregation = 0;

SELECT count() FROM
(
    SELECT concat(toString(number % 50000), repeat('x', 60)) AS k
    FROM numbers_mt(1000000)
    GROUP BY k
);

SELECT 'no release without a spill decision', sumIf(value, event = 'AdaptiveAggregationResidueReleases') = 0 FROM system.events;
SELECT 'residue was there to leave alone', sumIf(value, event = 'AdaptiveAggregationPressureSweeps') > 0 FROM system.events;
SELECT 'thawed onto the baseline path', sumIf(value, event = 'AdaptiveAggregationThaws') > 0 FROM system.events;
"

# The scenarios above aggregate keys only, so no aggregate state travels through the release. A lone
# count() is staged as an inline counter rather than as an aggregate state, so this scenario carries a
# second aggregate: with two of them the records stage real states, and both exact totals move if a
# state contribution is lost or corrupted. The memory limit is deliberately not pinned here - live
# states raise the spilled volume, and this scenario checks results, not the limit. The shape is the
# first scenario's: the states widen the residue, and it has to stay under the part bound so that the
# release, not a sweep, is what writes it.
$CLICKHOUSE_LOCAL --query "
SET max_threads = 4;
SET max_block_size = 8192;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 1000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 1000;
SET group_by_two_level_threshold_bytes = 1000000;
SET max_bytes_before_external_group_by = 20000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET collect_hash_table_stats_during_aggregation = 0;

SELECT count(), sum(s), sum(c) FROM
(
    SELECT concat(toString(number % 50000), repeat('x', 60)) AS k, sum(number) AS s, count() AS c
    FROM numbers_mt(1000000)
    GROUP BY k
);

SELECT 'released with live states', sumIf(value, event = 'AdaptiveAggregationResidueReleases') > 0 FROM system.events;
SELECT 'thawed onto the baseline path', sumIf(value, event = 'AdaptiveAggregationThaws') > 0 FROM system.events;
"
