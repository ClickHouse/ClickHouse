#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `adaptive_aggregator_disable_thaw` keeps the frozen tables of the adaptive aggregator frozen until the input ends.
# The source repeats each of 20000 keys a hundred times over the whole input, so the rows that miss the frozen tables
# keep repeating the same keys, and by default the aggregation thaws. Every cell compares the result with the one of
# the feature off and reports whether the aggregation thawed and whether it staged records. The cells cover a count,
# a general aggregate and a String key and state, with the thaw allowed and disabled.
#
# The thaw verdict is remembered in the hash-table statistics, which the first cells switch off so that every run
# measures. The last cell keeps them on and runs the same query four times in one process: the first run thaws and
# records the verdict, the second is refused the adaptive aggregator, the third, with the thaw disabled, freezes its
# tables anyway, and the fourth is refused again, because a run that may not thaw records no verdict. Each cell runs
# in its own clickhouse-local process, so the counters in `system.events` belong to it alone.

SETTINGS="SET max_threads = 4, max_block_size = 8192, adaptive_aggregator_freeze_threshold = 128, enable_adaptive_aggregator = 1;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0, optimize_injective_functions_in_group_by = 0;"

function check()
{
    local label=$1 disable_thaw=$2 query=$3
    $CLICKHOUSE_LOCAL --query "
    $SETTINGS
    SET collect_hash_table_stats_during_aggregation = 0, adaptive_aggregator_disable_thaw = $disable_thaw;

    SELECT '$label', $disable_thaw,
        (SELECT sum(cityHash64(*)) FROM ($query)) = (SELECT sum(cityHash64(*)) FROM ($query SETTINGS enable_adaptive_aggregator = 0));
    SELECT (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws') > 0,
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords') > 0;
    " | paste -sd '\t'
}

for disable_thaw in 0 1
do
    check 'Count' $disable_thaw "SELECT toUInt64(number % 20000) AS k, count() AS c FROM numbers_mt(2000000) GROUP BY k"
    check 'Sum' $disable_thaw "SELECT toUInt64(number % 20000) AS k, sum(number) AS s FROM numbers_mt(2000000) GROUP BY k"
    check 'String key and state' $disable_thaw \
        "SELECT toString(number % 20000) AS k, min(toString(number)) AS s FROM numbers_mt(2000000) GROUP BY k"
done

QUERY="SELECT toUInt64(number % 20000) AS k, count() AS c FROM numbers_mt(2000000) GROUP BY k"
COUNTERS="SELECT groupArray(value) FROM (SELECT event, coalesce(sum(value), 0) AS value FROM system.events
    WHERE event IN ('AdaptiveAggregationLocalFreezes', 'AdaptiveAggregationThaws') GROUP BY event ORDER BY event)"
$CLICKHOUSE_LOCAL --query "
$SETTINGS
SET collect_hash_table_stats_during_aggregation = 1;

$QUERY FORMAT Null;
CREATE TEMPORARY TABLE after_first ENGINE = Memory AS $COUNTERS;
$QUERY FORMAT Null;
CREATE TEMPORARY TABLE after_second ENGINE = Memory AS $COUNTERS;
$QUERY SETTINGS adaptive_aggregator_disable_thaw = 1 FORMAT Null;
CREATE TEMPORARY TABLE after_third ENGINE = Memory AS $COUNTERS;
$QUERY FORMAT Null;
CREATE TEMPORARY TABLE after_fourth ENGINE = Memory AS $COUNTERS;

-- Each array is [local freezes, thaws].
SELECT 'Remembered verdict',
    (SELECT * FROM after_first)[1] > 0 AND (SELECT * FROM after_first)[2] > 0,
    (SELECT * FROM after_second) = (SELECT * FROM after_first),
    (SELECT * FROM after_third)[1] > (SELECT * FROM after_second)[1] AND (SELECT * FROM after_third)[2] = (SELECT * FROM after_second)[2],
    (SELECT * FROM after_fourth) = (SELECT * FROM after_third);
"
