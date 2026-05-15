#!/usr/bin/env bash
# Tags: use-xray, no-parallel
# no-parallel: avoid other tests trying to add the same instrumentation to the same symbol
#
# How the bug manifests:
# - The ordered_non_unique index sorts elements by (function_id, id)
# - find(func_id) returns an iterator to the first matching element
# - The loop continues until end(), processing ALL subsequent elements
# - This means calling a function with a lower function_id incorrectly triggers
#   handlers for functions with higher function_ids

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM INSTRUMENT REMOVE ALL;"
}

trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "SYSTEM INSTRUMENT REMOVE ALL;"

$CLICKHOUSE_CLIENT -q "
    SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG ENTRY 'marker_start';
    SYSTEM INSTRUMENT ADD 'QueryMetricLog::finishQuery' LOG ENTRY 'marker_finish';
"

query_id="${CLICKHOUSE_DATABASE}_dispatch_bug"
$CLICKHOUSE_CLIENT --query-id="$query_id" -q "SELECT 1 FORMAT Null;"

$CLICKHOUSE_CLIENT -q "SYSTEM INSTRUMENT REMOVE ALL;"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS system.trace_log;"

# The key assertion: each function should have its handler called EXACTLY ONCE.
# With the bug, if one function has a lower function_id than another,
# calling the lower one also triggers the higher one's handler.
$CLICKHOUSE_CLIENT -q "
    SELECT '-- Each function handler should be called exactly once:';
    SELECT 'startQuery count:', count()
    FROM system.trace_log
    WHERE event_date >= yesterday()
      AND query_id = '$query_id'
      AND trace_type = 'Instrumentation'
      AND handler = 'log'
      AND function_name LIKE '%QueryMetricLog::startQuery%';

    SELECT 'finishQuery count:', count()
    FROM system.trace_log
    WHERE event_date >= yesterday()
      AND query_id = '$query_id'
      AND trace_type = 'Instrumentation'
      AND handler = 'log'
      AND function_name LIKE '%QueryMetricLog::finishQuery%';
"
