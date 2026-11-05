#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Records staged before the thaw stay published for the merge-time drain, so a producer the thaw put
# back on the baseline path keeps holding them for the rest of the query and cannot free them by
# flushing its own table. The baseline spill decision is read from query-wide memory, so that
# residency keeps the threshold crossed on every following block, and each block leaves a temporary
# file holding a single block's keys. The frozen path shed the backlog under the same trigger
# already; the thawed one has to as well.
#
# The stream is the one from 05054: repeat-dominated wide keys freeze the tables, and the staged
# stream then proves repeat-dominated and thaws them. Two producers keep the whole backlog resident.
# The thaw cannot fire before 524288 staged records, which at these key widths is about 42 MB of
# them, so the threshold has to sit above that or the sweeps shed the backlog before the thaw and
# there is nothing left to stay resident.
#
# What carries the query back over the threshold after the thaw is the growth of the two baseline
# tables the thaw hands the producers, on top of the resident backlog, so the key space is wide
# enough that this growth is several times the margin the backlog leaves rather than comparable to
# it. Measured on this tree, the shedding then still fires over the whole 30-80 MB band of
# thresholds, instead of only the 42-66 MB the 50000-key stream held it in, where a producer that
# ran a little ahead of its twin could let the frozen sweeps shed the backlog before the thaw and
# leave nothing resident. That narrow band is what made this test flaky in CI.
#
# What the assertions bound is the part count, not the peak: measured with this binary the resident
# backlog costs 365-390 parts against 5-7 with it shed, while the peak differs by half a threshold.
# The memory limit is left as generous as the rest of the family's, so that a sanitizer arm, which
# pays several times over per part written and read back, cannot fail on the peak alone; the shed
# arm peaks under 100 MB here.
#
# The query runs in its own clickhouse-local process, so the counters in `system.events` belong to
# it alone.
$CLICKHOUSE_LOCAL --query "
SET max_threads = 2;
SET max_block_size = 8192;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 1000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 1000;
SET group_by_two_level_threshold_bytes = 1000000;
SET max_bytes_before_external_group_by = 56000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_memory_usage = 300000000;
-- The hash-table statistics remember the thaw verdict and a marked query skips the adaptive
-- engagement, so without this only the first run of the shape would reach it.
SET collect_hash_table_stats_during_aggregation = 0;

SELECT count() FROM
(
    SELECT concat(toString(number % 100000), repeat('x', 60)) AS k
    FROM numbers_mt(4000000)
    GROUP BY k
);

-- Parts are written only once query memory crosses the external threshold, which is the condition
-- the shedding is guarded by, so this is what proves the baseline spill decision was reached.
SELECT 'went external', sumIf(value, event = 'ExternalAggregationWritePart') > 0 FROM system.events;
-- One part per block is the defect: a producer that finds the backlog resident on every block
-- writes its own few keys out on every block. 4000000 rows in blocks of 8192 is 488 blocks, and an
-- eighth of that leaves the handful of parts a shed backlog costs a wide margin.
SELECT 'parts stay far below the block count',
       sumIf(value, event = 'ExternalAggregationWritePart') * 8 < intDiv(4000000, 8192)
FROM system.events;
-- Without these the test could pass by never engaging the adaptive aggregator, or by never leaving
-- the frozen path, whose shedding is not what this covers.
SELECT 'thawed onto the baseline path', sumIf(value, event = 'AdaptiveAggregationThaws') > 0 FROM system.events;
SELECT 'swept under pressure', sumIf(value, event = 'AdaptiveAggregationPressureSweeps') > 0 FROM system.events;
"

# The three streams below each reach the hook on a shape of their own, and each of them ends by
# reading its counter into the pooled assertion at the end of the file rather than asserting on it
# here. Which producer arrives at the baseline spill trigger first is a race, so a single stream
# reads the counter as a coin flip even when the backlog does end up shed, and three separate
# reads redden three times as often as one. Pooling costs nothing that is measured elsewhere: the
# part bound stays on the first stream, and each of these three keeps the claim only it can make -
# no two-level table on the single-level stream, no thaw at all on the mixed one. It is not a way
# for something else to satisfy the read either, because none of these shapes reaches a non-zero
# counter without the hook: with the hook reverted all three read 0.
shed_total=0

# The part bound above needs the shedding to be rare, because every crossing of the threshold writes
# one part, and a shape that crosses often puts the two arms' part counts on top of each other. The
# hook's own observable needs the opposite: where the crossing happens only in the first blocks after
# the thaw, one missed crossing leaves the counter at zero for the whole run. So it is read over
# a second stream, at a quarter of the threshold, where the crossing recurs on most blocks and a
# thawed producer therefore reaches the hook on the block it thaws on. The counter still lands at one
# shed per producer, because the backlog is shed once and a later crossing finds it gone: measured
# here it is exactly 2 in 23 runs of 23, sequential and eight at a time alike. The stream is halved
# because this query bounds nothing and only has to reach the hook, and it peaks lower than the
# first one.
#
# Its own process again: `system.events` is cumulative for the whole process, so run together with
# the stream above this read would return non-zero whenever the hook fires only there, and it would
# pin nothing about this shape. That is also why the pooling is over the three per-stream reads and
# not over one process running all three.
out=$($CLICKHOUSE_LOCAL --query "
SET max_threads = 2;
SET max_block_size = 8192;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 1000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 1000;
SET group_by_two_level_threshold_bytes = 1000000;
SET max_bytes_before_external_group_by = 16000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_memory_usage = 300000000;
SET collect_hash_table_stats_during_aggregation = 0;

SELECT 'liveness stream', count() FROM
(
    SELECT concat(toString(number % 100000), repeat('x', 60)) AS k
    FROM numbers_mt(2000000)
    GROUP BY k
);

-- This event is incremented only from the new hook, on the baseline memory-pressure path in
-- Aggregator::executeOnBlock, and only when the sweep it runs there took staged records out, so it
-- proves a shed happened rather than that the trigger was reached. The liveness assertions above are
-- satisfiable by the pre-existing finish-time drain as well, and the part bound is the oracle; this
-- one pins the hook itself.
SELECT sumIf(value, event = 'AdaptiveAggregationSpillBacklogSheds') FROM system.events;
")
# The counter is the last line of the stream's output, and it is the only line held back from it.
printf '%s\n' "${out%$'\n'*}"
shed_total=$(( shed_total + ${out##*$'\n'} ))

# The freeze thresholds sit far below the two-level ones, so a thawed producer can carry the whole
# backlog while its own table is still single-level and unspillable. Waiting for the conversion
# would leave the backlog resident across the limit checks, which is why the shedding hangs off the
# memory-pressure trigger rather than off the spill branch. Here the conversion is put out of reach
# entirely: the hook is the only thing that can shed after the thaw, and with it gated behind the
# conversion nothing does - measured with this binary, the hook sheds 3-4 times per run, and with it
# reverted the same query holds ~99 MiB and dies under a 100 MB limit in 2 runs out of 5. The limit
# is left generous here because what this stream contributes is the counter, not the peak.
#
# A separate process again, so that its read cannot be satisfied by the streams above.
out=$($CLICKHOUSE_LOCAL --query "
SET max_threads = 2;
SET max_block_size = 8192;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 1000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 100000000;
SET group_by_two_level_threshold_bytes = 100000000000;
SET max_bytes_before_external_group_by = 56000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_memory_usage = 300000000;
SET collect_hash_table_stats_during_aggregation = 0;

SELECT 'single-level stream', count() FROM
(
    SELECT concat(toString(number % 100000), repeat('x', 60)) AS k
    FROM numbers_mt(4000000)
    GROUP BY k
);

SELECT 'thawed onto the baseline path', sumIf(value, event = 'AdaptiveAggregationThaws') > 0 FROM system.events;
SELECT sumIf(value, event = 'AdaptiveAggregationSpillBacklogSheds') FROM system.events;
")
printf '%s\n' "${out%$'\n'*}"
shed_total=$(( shed_total + ${out##*$'\n'} ))

# The hook is gated on the baseline phase and not on the thaw that motivated it, because the backlog
# is session-wide memory and whichever producer arrives at the spill trigger is the right one to shed
# it. So it fires with no thaw at all, which is what this stream pins: one producer freezes and
# stages a backlog, another gives up on freezing by the row rule - 16 times the freeze threshold in
# rows while staying below it in keys - and, once the query crosses the external threshold, sheds the
# frozen one's backlog from the baseline path. Narrowing the gate to the thaw verdict would leave
# that backlog resident for the rest of the query, which is the same defect this pull request is
# about; the event is therefore named after the baseline path rather than after the thaw.
#
# The two producers get their own key streams from the two `UNION ALL` branches, one stream each:
#
#  - the frozen one is distinct per row, so the staged sample is never repeat-dominated and the thaw
#    cannot fire, and it stages about 32 MB of records - below the threshold, so no sweep of its own
#    sheds them - and then ends, after which nothing but the hook can shed the backlog;
#  - the other holds 10 keys over its first 100000 rows and therefore gives up after about 16000 of
#    them, long before any memory pressure, and only then turns key-rich, so what carries the query
#    over the threshold is a baseline table growing after the frozen producer is gone.
#
# Measured with this binary the hook sheds once and takes the whole backlog with it.
#
# A separate process again, so that its counters cannot be satisfied by the streams above.
out=$($CLICKHOUSE_LOCAL --query "
SET max_threads = 2;
SET max_block_size = 8192;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 1000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 1000;
SET group_by_two_level_threshold_bytes = 1000000;
SET max_bytes_before_external_group_by = 56000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_memory_usage = 300000000;
SET collect_hash_table_stats_during_aggregation = 0;

SELECT 'mixed stream', count() FROM
(
    SELECT k, count() FROM
    (
        SELECT concat(toString(number), repeat('x', 60)) AS k FROM numbers(500000)
        UNION ALL
        SELECT concat(if(number < 100000, toString(number % 10), toString(number + 1000000000)), repeat('x', 60)) AS k
        FROM numbers(1000000)
    )
    GROUP BY k
);

SELECT 'no thread thawed', sumIf(value, event = 'AdaptiveAggregationThaws') = 0 FROM system.events;
SELECT 'a producer gave up on freezing', sumIf(value, event = 'AdaptiveAggregationGiveUps') > 0 FROM system.events;
SELECT sumIf(value, event = 'AdaptiveAggregationSpillBacklogSheds') FROM system.events;
")
printf '%s\n' "${out%$'\n'*}"
shed_total=$(( shed_total + ${out##*$'\n'} ))

# Naming the streams the read was pooled over, so that a red run does not have to work out from the
# source that the line is a disjunction and which shapes it covers.
printf 'shed the backlog on the baseline pressure path in at least one of the liveness, single-level and mixed streams\t%d\n' \
    "$(( shed_total > 0 ))"
