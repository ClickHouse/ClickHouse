#!/usr/bin/env bash
# Tags: use_xray, no-parallel
# no-parallel: avoid other tests interfering with the global system.instrumentation table

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
    SYSTEM INSTRUMENT ADD \`QueryMetricLog::startQuery\` PROFILE;
"""

query_id="${CLICKHOUSE_DATABASE}_profile"
$CLICKHOUSE_CLIENT --query-id="$query_id" -q "SELECT 1 FORMAT Null;"

$CLICKHOUSE_CLIENT -q """
    SYSTEM INSTRUMENT REMOVE ALL;
    SYSTEM FLUSH LOGS system.trace_log;
    SELECT entry_type FROM system.trace_log WHERE event_date >= yesterday() AND query_id = '$query_id' AND trace_type = 'Instrumentation' AND handler = 'profile' AND function_name ILIKE '%QueryMetricLog::startQuery%' AND NOT empty(trace);
"""
