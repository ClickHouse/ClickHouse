#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The hash-table statistics cache keys an aggregation by a hash of its serialized plan, which carries what a
# function captured when it was built (`IFunctionBase::updateHash`):
# - `visibleWidth` captures `function_visible_width_behavior`, so two sessions that differ in it must not share
#   an entry;
# - a conversion captures the format settings by the hash of their effective values, so a session that spells
#   a format setting at its default explicitly must share the entry of a session that left it alone.
#
# clickhouse-local gives a fresh process, so the process-global `AggregationPreallocatedElementsInHashTables`
# event starts at zero. The group count (650e3) must stay above the 500e3 lower bound under which
# `getSizeHint` does not preallocate at all; external aggregation would stop the stats collection, so both
# spill thresholds are pinned to 0 (see 05228_hash_table_stats_cache_conversion_settings). `visibleWidth(s)` is
# a function of the other key, so `optimize_group_by_function_keys` is off to keep it in the plan.

$CLICKHOUSE_LOCAL \
    --enable_analyzer=1 \
    --optimize_aggregation_in_order=0 \
    --optimize_group_by_function_keys=0 \
    --collect_hash_table_stats_during_aggregation=1 \
    --max_size_to_preallocate_for_aggregation=1000000000000 \
    --max_threads=1 \
    --max_bytes_before_external_group_by=0 \
    --max_bytes_ratio_before_external_group_by=0 \
    -q "
    CREATE TABLE t_stats (s String) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_stats SELECT toString(number) FROM numbers(650000);

    -- The session at the defaults writes its statistics.
    SELECT s, visibleWidth(s) FROM t_stats GROUP BY 1, 2 FORMAT Null;

    -- The other session must not find them under its own key.
    SELECT s, visibleWidth(s) FROM t_stats GROUP BY 1, 2 FORMAT Null SETTINGS function_visible_width_behavior = 0;
    SELECT 'visibleWidth, other session', sum(value) FROM system.events WHERE event = 'AggregationPreallocatedElementsInHashTables';

    -- The session at the defaults writes its statistics.
    SELECT toFloat64(s) FROM t_stats GROUP BY 1 FORMAT Null;

    -- A session that spells a format setting at its default is the same session for the key.
    SELECT toFloat64(s) FROM t_stats GROUP BY 1 FORMAT Null SETTINGS output_format_json_quote_denormals = 0;
    SELECT 'conversion, default spelled explicitly', sum(value) > 0 FROM system.events WHERE event = 'AggregationPreallocatedElementsInHashTables';
"
