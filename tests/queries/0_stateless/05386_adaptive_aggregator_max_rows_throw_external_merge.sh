#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Once a frozen table of the adaptive aggregator is written to disk, the merge goes external: it merges
# the parts of the tables with the staged records, which sources feed into it as converted buckets. Each
# producer holds a throw-mode `max_rows_to_group_by` against an estimate of its own keys, and the sources
# hold it against the staged groups of the buckets they merged: a lower bound on the final group count.
# The query reads four `UNION ALL` lanes, one per producer. A lane first repeats 250 hot keys, whose
# growing `groupArray` states get its frozen tables written, and then brings 20000 cold keys of its own,
# which its frozen table stages. A threshold of one byte keeps the query over it on every block. The
# thresholds are pinned because the runner randomizes them, and each cell runs in its own
# clickhouse-local process, so the counters in `system.events` belong to it alone.
SETTINGS_COMMON="
SET max_threads = 4;
SET max_threads_min_free_memory_per_thread = 0;
SET max_block_size = 8192;
SET adaptive_aggregator_freeze_threshold = 128;
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
    SELECT if(number % 1000000000 < 200000, number % 250, number) AS g, groupArray(number) AS a FROM (
        SELECT number FROM numbers(1000000000, 220000) UNION ALL SELECT number FROM numbers(2000000000, 220000)
        UNION ALL SELECT number FROM numbers(3000000000, 220000) UNION ALL SELECT number FROM numbers(4000000000, 220000))
    GROUP BY g)"

# A limit of exactly the 80250 groups does not fire, because the staged groups never count past the final
# group count. The counters show that the run merged externally.
$CLICKHOUSE_LOCAL --query "
$SETTINGS_COMMON
SELECT 'A limit at the group count leaves the result exact';
$QUERY SETTINGS max_rows_to_group_by = 80250;
SELECT 'The frozen tables went to disk and the merge went external';
SELECT
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationFrozenTableSpills') > 0,
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ExternalAggregationMerge') > 0;
"

# Nearly all of the 80000 cold keys are staged, over the limit of 50000, while each producer holds 20250
# keys, too few for its own check, so the sources of the external merge throw.
echo "A limit below the staged group count aborts the external merge"
$CLICKHOUSE_LOCAL --query "
$SETTINGS_COMMON
$QUERY SETTINGS max_rows_to_group_by = 50000;
" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -1
