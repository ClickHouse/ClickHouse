#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The hash-table statistics cache keys an aggregation by a hash of its serialized plan, where a function
# node used to be written by its name alone: two sessions that differ only in a setting the conversion
# captured (`precise_float_parsing` here) shared one entry, and the second session preallocated its
# hash table from the first one's statistics. The key now carries the hash of what the function
# captured (`IFunctionBase::updateHash`), so the second session's first run finds no entry.
#
# clickhouse-local gives a fresh process, so the process-global AggregationPreallocatedElementsInHashTables
# event starts at zero. The group count (650e3) must stay above the 500e3 lower bound under which
# getSizeHint does not preallocate at all; external aggregation would stop the stats collection, so both
# spill thresholds are pinned to 0 (see 04625_hash_table_sizes_stats_table_expression_modifiers).
# The adaptive aggregator also takes one thread and records no table sizes in the statistics, so it is off.

$CLICKHOUSE_LOCAL \
    --optimize_aggregation_in_order=0 \
    --collect_hash_table_stats_during_aggregation=1 \
    --max_size_to_preallocate_for_aggregation=1000000000000 \
    --max_threads=1 \
    --enable_adaptive_aggregator=0 \
    --max_bytes_before_external_group_by=0 \
    --max_bytes_ratio_before_external_group_by=0 \
    -q "
    CREATE TABLE t_stats (s String) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_stats SELECT toString(number) FROM numbers(650000);

    -- The session at the defaults writes its statistics.
    SELECT toFloat64(s) FROM t_stats GROUP BY 1 FORMAT Null;

    -- The other session must not find them under its own key.
    SELECT toFloat64(s) FROM t_stats GROUP BY 1 FORMAT Null SETTINGS precise_float_parsing = 0;
    SELECT 'preallocated after the other session', sum(value) FROM system.events WHERE event = 'AggregationPreallocatedElementsInHashTables';

    -- A repeat of it does: the key tells the sessions apart without becoming unique to a run.
    SELECT toFloat64(s) FROM t_stats GROUP BY 1 FORMAT Null SETTINGS precise_float_parsing = 0;
    SELECT 'preallocated after a repeat', sum(value) > 0 FROM system.events WHERE event = 'AggregationPreallocatedElementsInHashTables';
"

# `date_time_output_format` changes what `toString` of an `Array(DateTime)` returns, and the key sees it only through
# the hash of the changed format settings (`getFormatSettingsHash`): each of its values gets a key of its own.
# `optimize_injective_functions_in_group_by` is off, or the aggregation key would be `d` itself.
$CLICKHOUSE_LOCAL \
    --optimize_aggregation_in_order=0 \
    --optimize_injective_functions_in_group_by=0 \
    --collect_hash_table_stats_during_aggregation=1 \
    --max_size_to_preallocate_for_aggregation=1000000000000 \
    --max_threads=1 \
    --max_bytes_before_external_group_by=0 \
    --max_bytes_ratio_before_external_group_by=0 \
    -q "
    CREATE TABLE t_dt (d Array(DateTime('UTC'))) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_dt SELECT [toDateTime(number, 'UTC')] FROM numbers(650000);

    SELECT toString(d) FROM t_dt GROUP BY 1 FORMAT Null;
    SELECT toString(d) FROM t_dt GROUP BY 1 FORMAT Null SETTINGS date_time_output_format = 'iso';
    SELECT 'preallocated after a changed format setting', sum(value) FROM system.events WHERE event = 'AggregationPreallocatedElementsInHashTables';

    SELECT toString(d) FROM t_dt GROUP BY 1 FORMAT Null SETTINGS date_time_output_format = 'unix_timestamp';
    SELECT 'preallocated after another value of it', sum(value) FROM system.events WHERE event = 'AggregationPreallocatedElementsInHashTables';

    SELECT toString(d) FROM t_dt GROUP BY 1 FORMAT Null SETTINGS date_time_output_format = 'unix_timestamp';
    SELECT 'preallocated after a repeat of it', sum(value) > 0 FROM system.events WHERE event = 'AggregationPreallocatedElementsInHashTables';
"
