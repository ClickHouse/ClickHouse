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

$CLICKHOUSE_CLIENT -q "
    SYSTEM INSTRUMENT REMOVE ALL;
    SYSTEM INSTRUMENT ADD \`QueryMetricLog::startQuery\` LOG EXIT 'this is an instrumentation log';
"

query_id="${CLICKHOUSE_DATABASE}_log"
$CLICKHOUSE_CLIENT --query-id=$query_id -q "SELECT 1 FORMAT Null;"

$CLICKHOUSE_CLIENT -q "
    SYSTEM INSTRUMENT REMOVE ALL;
    SYSTEM FLUSH LOGS system.text_log;
    SELECT count() FROM system.text_log WHERE event_date >= yesterday() AND query_id = '$query_id' AND message ILIKE '%this is an instrumentation log%Stack trace:%';
"
