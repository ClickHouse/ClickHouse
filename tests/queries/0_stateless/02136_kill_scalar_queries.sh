#!/usr/bin/env bash
# Ref: https://github.com/ClickHouse/ClickHouse/issues/1576
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function wait_for_query_to_start()
{
    while [[ $($CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT count() FROM system.processes WHERE query_id = '$1'") == 0 ]]; do sleep 0.1; done
}

QUERY_1_ID="${CLICKHOUSE_DATABASE}_TEST02132KILL_QUERY1"
(${CLICKHOUSE_CLIENT} --query_id="${QUERY_1_ID}" --query='select (SELECT max(number) from system.numbers) + 1;'  2>&1 | grep -q "Code: 394." || echo 'FAIL') &
wait_for_query_to_start "${QUERY_1_ID}"
${CLICKHOUSE_CLIENT} --query="KILL QUERY WHERE query_id='${QUERY_1_ID}' SYNC"

QUERY_2_ID="${CLICKHOUSE_DATABASE}_TEST02132KILL_QUERY2"
(${CLICKHOUSE_CLIENT} --query_id="${QUERY_2_ID}" --query='SELECT (SELECT number FROM system.numbers WHERE number = 1000000000000);'  2>&1 | grep -q "Code: 394." || echo 'FAIL') &
wait_for_query_to_start "${QUERY_2_ID}"
${CLICKHOUSE_CLIENT} --query="KILL QUERY WHERE query_id='${QUERY_2_ID}' SYNC"

wait
