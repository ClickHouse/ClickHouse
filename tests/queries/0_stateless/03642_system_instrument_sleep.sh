#!/usr/bin/env bash
# Tags: use_xray, no-parallel, no-fasttest
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
    SYSTEM INSTRUMENT ADD \`QueryMetricLog::startQuery\` SLEEP ENTRY 3.2;
"""

query_id="${CLICKHOUSE_DATABASE}_sleep"
$CLICKHOUSE_CLIENT --query-id="$query_id" -q "SELECT 1 FORMAT Null;"

$CLICKHOUSE_CLIENT -q """
    SYSTEM INSTRUMENT REMOVE ALL;
    SYSTEM FLUSH LOGS system.query_log;
    SELECT query_duration_ms > 2000 FROM system.query_log WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type > 1 AND query_id = '$query_id';
"""
