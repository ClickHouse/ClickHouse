#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `max_rows_to_group_by` in throw mode is admitted by the adaptive aggregator: the limit drops
# nothing, so the frozen tables and the staging stay exact, and the query only has to abort once
# too many groups exist. The producers cannot see the staged keys' cardinality (their frozen
# tables are bounded by the freeze threshold), so the limit is enforced where the groups first
# materialize: as the bucket-parallel merge converts buckets. The dropping modes (break, any)
# stay rejected. Both external thresholds are pinned off because the ratio threshold defaults
# to a half of the memory limit: a pressured runner could spill, and a spilled run merges
# externally, where the limit is checked as the spill drains build their tables (its own cell
# below).
# The two-level thresholds are pinned high so the baseline spill branch stays out of the way.
# The test runs in one `clickhouse-local` process per cell, so the `system.events` counters
# belong to that cell alone.
SETTINGS_COMMON="
SET max_threads = 4;
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

# A limit below the group count must abort the query. The freeze threshold keeps every
# producer-local count far below the limit, so no producer-side check can fire; the throw can
# only come from the merge-time accounting.
echo "A limit below the group count aborts the merge"
$CLICKHOUSE_LOCAL --query "
$SETTINGS_COMMON
SELECT count() FROM (SELECT number % 200000 AS g, count() AS c FROM numbers_mt(600000) GROUP BY g SETTINGS max_rows_to_group_by = 50000);
" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -1

# The bucket-local Top-K conversion truncates each converted bucket to its n best groups, so the
# limit accounting reads the bucket table's group count instead of the chunk's row count; a
# chunk-based count would never reach the limit under the truncation and the query would wrongly
# succeed. The plan settings the rewrite depends on are pinned on, so the cell exercises the
# truncating conversion regardless of randomization.
echo "A bucket Top-K plan still hits the limit"
$CLICKHOUSE_LOCAL --query "
$SETTINGS_COMMON
SET query_plan_enable_optimizations = 1;
SET query_plan_push_down_limit = 1;
SET query_plan_aggregation_bucket_top_k = 1;
SET max_rows_to_group_by = 50000;
SELECT g, c FROM (SELECT number % 200000 AS g, count() AS c FROM numbers_mt(600000) GROUP BY g ORDER BY c DESC LIMIT 10);
" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -1

# A spilled run merges through the external machinery instead of the counted in-memory merge,
# so the spill drains hold the limit against their drain tables as they build them, bucket by
# bucket. The one-byte threshold keeps the pressure valve draining for the whole query, and the
# staged keys exceed the limit many times over, so the first drain batch must abort the query.
# The reported group count pins the per-bucket granularity: the abort must come within a few
# buckets' worth of keys past the limit, not after a whole floor-sized batch (which would
# report close to a million).
echo "A spilling run hits the limit within a few buckets past it"
$CLICKHOUSE_LOCAL --query "
$SETTINGS_COMMON
SET max_bytes_before_external_group_by = 1;
SELECT count() FROM (SELECT number % 1300000 AS g, count() AS c FROM numbers_mt(2600000) GROUP BY g SETTINGS max_rows_to_group_by = 100000);
" 2>&1 | grep -oE "has [0-9]+ rows, maximum: 100000" | head -1 | \
    awk '{ print ($2 > 100000 && $2 < 150000) ? "aborted near the limit" : "aborted at " $2 }'

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
