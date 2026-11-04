#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Once a frozen table of the adaptive aggregator is written to disk, the merge goes external: it merges
# the parts of the tables with the staged records, which sources feed into it as converted buckets. The
# producers cannot hold a throw-mode `max_rows_to_group_by`, because the freeze threshold bounds their
# tables, so the sources hold it against the staged groups of the buckets they merged: a lower bound on
# the final group count. Even numbers fall on 250 hot keys, whose growing `groupArray` states get their
# tables written, and odd numbers are cold keys, which are staged. A threshold of one byte keeps the query
# over it on every block. The thresholds are pinned because the runner randomizes them, and each cell
# runs in its own clickhouse-local process, so the counters in `system.events` belong to it alone.
SETTINGS_COMMON="
SET max_threads = 4;
SET max_block_size = 8192;
SET adaptive_aggregator_freeze_threshold = 1000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 100000000;
SET group_by_two_level_threshold_bytes = 1000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 1;
SET max_bytes_ratio_before_external_group_by = 0;
SET enable_adaptive_aggregator = 1;
SET group_by_overflow_mode = 'throw';
"
QUERY="SELECT count(), sum(length(a)) FROM (
    SELECT if(number % 2 = 0, number % 500, number) AS g, groupArray(number) AS a FROM numbers_mt(1000000) GROUP BY g)"

# A limit of exactly the 500250 groups does not fire, because the staged groups never count past the final
# group count. The counters show that the run merged externally.
$CLICKHOUSE_LOCAL --query "
$SETTINGS_COMMON
SELECT 'A limit at the group count leaves the result exact';
$QUERY SETTINGS max_rows_to_group_by = 500250;
SELECT 'The frozen tables went to disk and the merge went external';
SELECT
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationFrozenTableSpills') > 0,
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ExternalAggregationMerge') > 0;
"

# Nearly all of the 500000 cold keys are staged, ten times the limit of 50000, and no producer-side check
# sees them.
echo "A limit below the staged group count aborts the external merge"
$CLICKHOUSE_LOCAL --query "
$SETTINGS_COMMON
$QUERY SETTINGS max_rows_to_group_by = 50000;
" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -1
