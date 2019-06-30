#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e -o pipefail

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL?query_id=hello&replace_running_query=1" -d 'SELECT sum(ignore(*)) FROM (SELECT number % 1000 AS k, groupArray(number) FROM numbers(100000000) GROUP BY k)' 2>&1 > /dev/null &
sleep 0.1 # First query (usually) should be received by the server after this sleep.
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL?query_id=hello&replace_running_query=1" -d 'SELECT 0'
wait

${CLICKHOUSE_CLIENT} --user=readonly --query_id=42 --query='SELECT 1, sleep(1)' &
sleep 0.1
( ${CLICKHOUSE_CLIENT} --query_id=42 --query='SELECT 43' ||: ) 2>&1 | grep -F 'is already running by user' > /dev/null
wait

${CLICKHOUSE_CLIENT} --query='SELECT 3, sleep(1)' &
sleep 0.1
${CLICKHOUSE_CLIENT} --query_id=42 --query='SELECT 2, sleep(1)' &
sleep 0.1
( ${CLICKHOUSE_CLIENT} --query_id=42 --replace_running_query=1 --queue_max_wait_ms=500 --query='SELECT 43' ||: ) 2>&1 | grep -F 'cant be stopped' > /dev/null
${CLICKHOUSE_CLIENT} --query_id=42 --replace_running_query=1 --query='SELECT 44'
wait
