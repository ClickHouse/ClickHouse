#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `max_rows_to_group_by` in throw mode is admitted by the adaptive aggregator: the limit drops
# nothing, so the frozen tables and the staging stay exact, and the query only has to abort once
# too many groups exist. Each producer estimates its own keys and throws while it reads once the
# estimate is well over the limit (`05895_adaptive_aggregator_max_rows_throw_distinct_key_counter`). The union
# of the producers' keys is held where the groups first materialize: as the bucket-parallel merge
# converts buckets. The merge cells read four `UNION ALL` lanes, one per producer, whose keys
# exceed the limit together while each lane stays under it. The dropping modes (break, any)
# stay rejected. Both external thresholds are pinned off, so that each cell runs the path it
# names: the ratio threshold defaults to a half of the memory limit, and a pressured runner could
# spill. A run that spills only its staged records keeps the bucket-parallel merge, which reads
# them back (its own cell below). A run whose frozen tables spill merges externally, which
# `05386_adaptive_aggregator_max_rows_throw_external_merge` covers.
# The two-level thresholds are pinned high so the baseline spill branch stays out of the way.
# The test runs in one `clickhouse-local` process per cell, so the `system.events` counters
# belong to that cell alone.
SETTINGS_COMMON="
SET max_threads = 4;
SET max_threads_min_free_memory_per_thread = 0;
SET max_block_size = 8192;
SET group_by_two_level_threshold = 10000000;
SET group_by_two_level_threshold_bytes = 500000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 128;
SET group_by_overflow_mode = 'throw';
"

# A limit above the group count must not change the result, and the run under it must really be
# an adaptive run: the freeze counter is what distinguishes an engaged run from a silently
# rejected one that fell back to the baseline.
$CLICKHOUSE_LOCAL --query "
$SETTINGS_COMMON
SELECT 'A limit above the group count leaves the adaptive result exact';
SELECT
    (SELECT count(), sum(c) FROM (SELECT number % 200000 AS g, count() AS c FROM numbers_mt(600000) GROUP BY g SETTINGS max_rows_to_group_by = 1000000))
    =
    (SELECT count(), sum(c) FROM (SELECT number % 200000 AS g, count() AS c FROM numbers_mt(600000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0, max_rows_to_group_by = 0));

SELECT 'The adaptive aggregator engaged under the limit';
SELECT coalesce(sum(value), 0) > 0 FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes';
"

# Four lanes of single-stream `numbers`, one per producer. Lane `i` reads `$1` numbers from `i`
# million on, so the lanes have disjoint keys.
function lanes()
{
    echo "(SELECT number AS g FROM numbers(0, $1) UNION ALL SELECT number FROM numbers(1000000, $1)
        UNION ALL SELECT number FROM numbers(2000000, $1) UNION ALL SELECT number FROM numbers(3000000, $1))"
}

# A limit below the group count must abort the query. Even a producer that read every lane
# would hold its 50400 keys within the margin of a sixteenth over the limit, under which neither
# its table nor its distinct key counter throws, so the throw can only come from the merge-time accounting.
echo "A limit below the group count aborts the merge"
$CLICKHOUSE_LOCAL --query "
$SETTINGS_COMMON
SELECT count() FROM (SELECT g, count() AS c FROM $(lanes 12600) GROUP BY g SETTINGS max_rows_to_group_by = 50000);
" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -1

# The bucket-local Top-K conversion truncates each converted bucket to its n best groups, so the
# limit accounting reads the bucket table's group count instead of the chunk's row count; a
# chunk-based count would never reach the limit under the truncation and the query would wrongly
# succeed. The lanes of the cell above leave the throw to the merge. The plan settings the
# rewrite depends on are pinned on, so the cell exercises the truncating conversion regardless of
# randomization.
echo "A bucket Top-K plan still hits the limit"
$CLICKHOUSE_LOCAL --query "
$SETTINGS_COMMON
SET query_plan_enable_optimizations = 1;
SET query_plan_push_down_limit = 1;
SET query_plan_aggregation_bucket_top_k = 1;
SET max_rows_to_group_by = 50000;
SELECT g, c FROM (SELECT g, count() AS c FROM $(lanes 12600) GROUP BY g ORDER BY c DESC LIMIT 10);
" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -1

# Over the threshold the producers write their staged records to disk, and the merge reads them
# back bucket by bucket, holding the limit against the groups merged so far. The one-byte
# threshold keeps the producers spilling for the whole query. Each lane stages 90000 keys of its
# own, under the limit, and together they exceed it four times over. The reported group count
# pins the per-bucket granularity: the abort must come within a few buckets' worth of keys past
# the limit, not after the whole stream (which would report close to 360000). A producer that
# read several lanes would throw on the estimate of its keys, which is near the limit as well.
echo "A spilling run hits the limit within a few buckets past it"
$CLICKHOUSE_LOCAL --query "
$SETTINGS_COMMON
SET max_bytes_before_external_group_by = 1;
SELECT count() FROM (SELECT g, count() AS c FROM $(lanes 90000) GROUP BY g SETTINGS max_rows_to_group_by = 100000);
" 2>&1 | grep -oE "has (about )?[0-9]+ rows, maximum: 100000" | head -1 | grep -oE "[0-9]+ rows" | \
    awk '{ print ($1 > 100000 && $1 < 150000) ? "aborted near the limit" : "aborted at " $1 }'

# The dropping modes are still rejected: a break-mode query must run on the baseline (no freeze
# ever happens), because they leave part of the input unaggregated once a table fills, which
# the staging has no counterpart for. The query itself only has to succeed; its row count under
# a firing break-mode limit is scheduling-dependent, so nothing is asserted about it.
$CLICKHOUSE_LOCAL --query "
$SETTINGS_COMMON
SET group_by_overflow_mode = 'break';
SELECT 'A dropping mode stays on the baseline';
SELECT count() FROM (SELECT number % 200000 AS g, count() AS c FROM numbers_mt(600000) GROUP BY g SETTINGS max_rows_to_group_by = 50000) FORMAT Null;
SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes';
"
