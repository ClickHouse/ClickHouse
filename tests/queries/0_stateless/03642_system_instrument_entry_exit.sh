#!/usr/bin/env bash
# Tags: use_xray, no-parallel, no-fasttest
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
    SYSTEM INSTRUMENT ADD \`QueryMetricLog::startQuery\` LOG ENTRY 'this is an entry log';
    SYSTEM INSTRUMENT ADD \`QueryMetricLog::startQuery\` LOG EXIT 'this is an exit log';
    SYSTEM INSTRUMENT ADD \`QueryMetricLog::startQuery\` SLEEP ENTRY 3;
    SYSTEM INSTRUMENT ADD \`QueryMetricLog::startQuery\` PROFILE;
"""

query_id="${CLICKHOUSE_DATABASE}_log"
$CLICKHOUSE_CLIENT --query-id=$query_id -q "SELECT 1 FORMAT Null;"

$CLICKHOUSE_CLIENT -q """
    SYSTEM INSTRUMENT REMOVE ALL;
    SYSTEM FLUSH LOGS system.trace_log, system.text_log;
    SELECT handler, entry_type FROM system.trace_log WHERE event_date >= yesterday() AND query_id = '$query_id' AND trace_type = 'Instrumentation' AND function_name ILIKE '%QueryMetricLog::startQuery%' AND NOT empty(trace) ORDER BY event_time_microseconds;
    SELECT (max(event_time_microseconds) - min(event_time_microseconds)) > 2 FROM system.text_log WHERE event_date >= yesterday() AND query_id = '$query_id' AND message ILIKE '%this is an%log%';
"""
