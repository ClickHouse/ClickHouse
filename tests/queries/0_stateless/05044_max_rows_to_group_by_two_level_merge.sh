#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A throw-mode `max_rows_to_group_by` is enforced against the merged group totals of the
# two-level in-memory merge. The producers' own checks see one table at a time, so a query whose
# per-producer key sets each stay under the limit while their union exceeds it used to succeed;
# with the bucket-local Top-K conversion the merged output does not even materialize the excess,
# so only counting the bucket tables at the merge can hold the limit. The keys are unique per
# row, so with 4 threads every producer holds roughly a quarter of them, far under the limit,
# and only the merged total crosses it. The two-level threshold is pinned low to force the
# two-level merge, and the external thresholds are pinned off because a spill diverts the finish
# away from it. The adaptive aggregator is disabled: these cells pin the baseline semantics, and
# its own limit behavior is covered by 05042.
SETTINGS_COMMON="
SET max_threads = 4;
SET max_block_size = 8192;
SET group_by_two_level_threshold = 1000;
SET group_by_two_level_threshold_bytes = 500000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET enable_adaptive_aggregator = 0;
SET group_by_overflow_mode = 'throw';
"

echo "A merged total above the limit aborts the merge"
$CLICKHOUSE_LOCAL --query "
$SETTINGS_COMMON
SET max_rows_to_group_by = 300000;
SELECT count() FROM (SELECT number AS g, count() AS c FROM numbers_mt(600000) GROUP BY g);
" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -1

# The bucket-local Top-K conversion truncates each converted bucket to its n best groups, so the
# merged output alone cannot reveal the crossing; the limit must be held against the bucket
# tables before the truncation.
echo "A bucket Top-K plan cannot hide the crossing"
$CLICKHOUSE_LOCAL --query "
$SETTINGS_COMMON
SET query_plan_enable_optimizations = 1;
SET query_plan_push_down_limit = 1;
SET query_plan_aggregation_bucket_top_k = 1;
SET max_rows_to_group_by = 300000;
SELECT g, c FROM (SELECT number AS g, count() AS c FROM numbers_mt(600000) GROUP BY g ORDER BY c DESC LIMIT 10);
" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -1

# A limit above the merged total must not fire, and the result must stay exact.
$CLICKHOUSE_LOCAL --query "
$SETTINGS_COMMON
SELECT 'A limit above the merged total leaves the result exact';
SELECT
    (SELECT count(), sum(c) FROM (SELECT number AS g, count() AS c FROM numbers_mt(600000) GROUP BY g SETTINGS max_rows_to_group_by = 1000000))
    =
    (SELECT count(), sum(c) FROM (SELECT number AS g, count() AS c FROM numbers_mt(600000) GROUP BY g SETTINGS max_rows_to_group_by = 0));
"

# A NULL group of a nullable single-column key lives in a dedicated slot of bucket 0's table
# rather than in an ordinary cell, and the bucket table's size accessor counts it, so the
# merged total must include it. The limit sits exactly one below the group count, and only the
# NULL group crosses it: an accounting that missed the slot would return the full result.
echo "The NULL group counts toward the limit"
$CLICKHOUSE_LOCAL --query "
$SETTINGS_COMMON
SET group_by_two_level_threshold = 1000;
SET max_rows_to_group_by = 599999;
SELECT count() FROM (SELECT nullIf(number, 0) AS g, count() AS c FROM numbers_mt(600000) GROUP BY g);
" 2>&1 | grep -oE "has [0-9]+ rows, maximum: [0-9]+" | head -1

# The dropping modes leave the merge untouched: their contract is decided at the producers, and
# every producer stays under the limit here, so the same union-over-limit query must succeed
# with the complete result.
$CLICKHOUSE_LOCAL --query "
$SETTINGS_COMMON
SET group_by_overflow_mode = 'break';
SELECT 'A break-mode limit does not stop the merge';
SELECT count() FROM (SELECT number AS g, count() AS c FROM numbers_mt(600000) GROUP BY g SETTINGS max_rows_to_group_by = 300000);
"
