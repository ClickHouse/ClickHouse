#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e -o pipefail

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL?query_id=hello&replace_running_query=1" -d 'SELECT sum(ignore(*)) FROM (SELECT number % 1000 AS k, groupArray(number) FROM numbers(100000000) GROUP BY k)' 2>&1 > /dev/null &
sleep 0.1 # First query (usually) should be received by the server after this sleep.
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL?query_id=hello&replace_running_query=1" -d 'SELECT 0'
wait
