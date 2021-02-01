#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

url="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?session_id=test_01194"
rnd=$RANDOM

${CLICKHOUSE_CURL} -sS "$url&query=SELECT+'test_01194',$rnd,1" > /dev/null
${CLICKHOUSE_CURL} -sS "$url&query=SELECT+'test_01194',$rnd,2" > /dev/null
${CLICKHOUSE_CURL} -sS "$url" --data "SELECT 'test_01194',$rnd,3" > /dev/null
${CLICKHOUSE_CURL} -sS "$url" --data "SELECT 'test_01194',$rnd,4" > /dev/null

${CLICKHOUSE_CURL} -sS "$url" --data "SYSTEM FLUSH LOGS"

${CLICKHOUSE_CURL} -sS "$url&query=SELECT+count(DISTINCT+query_id)+FROM+system.query_log+WHERE+query+LIKE+'SELECT+''test_01194'',$rnd%25'"
