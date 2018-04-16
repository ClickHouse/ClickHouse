#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e -o pipefail

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL?query_id=hello" -d 'SELECT sum(ignore(*)) FROM (SELECT number % 1000 AS k, groupArray(number) FROM numbers(20000000) GROUP BY k)' | wc -l &
sleep 0.1 # First query (usually) should be received by the server after this sleep.
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = 'hello' FORMAT Null"
wait
