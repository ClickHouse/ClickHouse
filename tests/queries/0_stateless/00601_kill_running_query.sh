#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e -o pipefail

function wait_for_query_to_start()
{
    while [[ $($CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT count() FROM system.processes WHERE query_id = '$1'") == 0 ]]; do sleep 0.1; done
}

${CLICKHOUSE_CURL_COMMAND} -q --max-time 30 -sS "$CLICKHOUSE_URL&query_id=test_00601_$CLICKHOUSE_DATABASE" -d 'SELECT sum(ignore(*)) FROM (SELECT number % 1000 AS k, groupArray(number) FROM numbers(50000000) GROUP BY k) SETTINGS max_rows_to_read = 0' > /dev/null &
wait_for_query_to_start "test_00601_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = 'test_00601_$CLICKHOUSE_DATABASE'"
wait
