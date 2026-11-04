#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The hash-table statistics cache keys an aggregation by a hash of its serialized plan, which carries what a
# function captured when it was built (`IFunctionBase::updateHash`). `JSONAllPaths` captures
# `type_json_skip_null_typed_paths`, so two sessions that differ in it must not share an entry.
#
# clickhouse-local gives a fresh process, so the process-global `AggregationPreallocatedElementsInHashTables`
# event starts at zero. See 05317_hash_table_stats_cache_captured_settings for the other settings.

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
    CREATE TABLE t_stats (s String, j JSON(a Nullable(UInt32))) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_stats SELECT toString(number), '{}' FROM numbers(650000);

    -- The session at the defaults writes its statistics.
    SELECT s, JSONAllPaths(j) FROM t_stats GROUP BY 1, 2 FORMAT Null;

    -- The other session must not find them under its own key.
    SELECT s, JSONAllPaths(j) FROM t_stats GROUP BY 1, 2 FORMAT Null SETTINGS type_json_skip_null_typed_paths = 1;
    SELECT 'JSONAllPaths, other session', sum(value) FROM system.events WHERE event = 'AggregationPreallocatedElementsInHashTables';

    -- The same session finds them.
    SELECT s, JSONAllPaths(j) FROM t_stats GROUP BY 1, 2 FORMAT Null;
    SELECT 'JSONAllPaths, same session', sum(value) > 0 FROM system.events WHERE event = 'AggregationPreallocatedElementsInHashTables';
"
