#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

rnd="$CLICKHOUSE_DATABASE"
url="${CLICKHOUSE_URL}&session_id=test_01194_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CURL} -sS "$url&query=SELECT+'test_01194',$rnd,1" > /dev/null
${CLICKHOUSE_CURL} -sS "$url&query=SELECT+'test_01194',$rnd,2" > /dev/null
${CLICKHOUSE_CURL} -sS "$url" --data "SELECT 'test_01194',$rnd,3" > /dev/null
${CLICKHOUSE_CURL} -sS "$url" --data "SELECT 'test_01194',$rnd,4" > /dev/null

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"

$CLICKHOUSE_CLIENT -q "
  SELECT
    count(DISTINCT query_id)
  FROM system.query_log
  WHERE
        current_database = currentDatabase()
    AND event_date >= yesterday()
    AND query LIKE 'SELECT ''test_01194'',$rnd%'
    AND query_id != queryID()"
