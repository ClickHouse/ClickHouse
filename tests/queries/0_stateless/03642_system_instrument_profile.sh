#!/usr/bin/env bash
# Tags: use-xray, no-parallel
# no-parallel: avoid other tests trying to add the same instrumentation to the same symbol

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM INSTRUMENT REMOVE ALL;"
}

trap cleanup EXIT

$CLICKHOUSE_CLIENT -q """
    SYSTEM INSTRUMENT REMOVE ALL;
    SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' PROFILE;
"""

query_id="${CLICKHOUSE_DATABASE}_profile"
$CLICKHOUSE_CLIENT --query-id="$query_id" -q "SELECT 1 FORMAT Null;"

$CLICKHOUSE_CLIENT -q "
    SELECT '-- Check ENTRY_AND_EXIT is present in system.instrumentation';
    SELECT entry_type FROM system.instrumentation;

    SYSTEM INSTRUMENT REMOVE ALL;
    SYSTEM FLUSH LOGS system.trace_log;

    SELECT '-- Check the Entrys and Exits';
    SELECT entry_type, duration_nanoseconds FROM system.trace_log WHERE event_date >= yesterday() AND query_id = '$query_id' AND trace_type = 'Instrumentation' AND handler = 'profile' AND entry_type = 'Entry' AND function_name LIKE '%QueryMetricLog::startQuery%' AND arrayExists(x -> x LIKE '%dispatchHandler%', symbols);
    SELECT entry_type, duration_nanoseconds > 0 FROM system.trace_log WHERE event_date >= yesterday() AND query_id = '$query_id' AND trace_type = 'Instrumentation' AND handler = 'profile' AND entry_type = 'Exit' AND function_name LIKE '%QueryMetricLog::startQuery%' AND arrayExists(x -> x LIKE '%dispatchHandler%', symbols);
"

query_id="${CLICKHOUSE_DATABASE}_profile_recursive"
$CLICKHOUSE_CLIENT -q "SYSTEM INSTRUMENT ADD 'DB::recursiveRemoveLowCardinality(std::__1::shared_ptr<DB::IDataType const> const&)' PROFILE;"
$CLICKHOUSE_CLIENT --query-id="$query_id" -q "SELECT arrayFold(acc, x -> acc + x, [1, 2, 3], 0::Int64) FORMAT Null;"

$CLICKHOUSE_CLIENT -q "
    SYSTEM INSTRUMENT REMOVE ALL;
    SYSTEM FLUSH LOGS system.trace_log;

    SELECT '-- Check a recursive function has Entrys and Exits';
    SELECT countIf(entry_type == 'Entry') > 5, countIf(entry_type == 'Exit') > 5 FROM system.trace_log WHERE event_date >= yesterday() AND query_id = '$query_id' AND trace_type = 'Instrumentation' AND handler = 'profile' AND function_name LIKE '%recursiveRemoveLowCardinality%' AND arrayExists(x -> x LIKE '%dispatchHandler%', symbols);
"
